import logging
import pathlib

import numpy as np
import pandas as pd

from jinja2 import Environment, Template

from airflow.sdk import Variable

from libsys_airflow.plugins.orafin.models import Invoice
from libsys_airflow.plugins.orafin.payments import models_converter
from libsys_airflow.plugins.shared.utils import dag_run_url, send_email_with_server_name

logger = logging.getLogger(__name__)


def _ap_report_errors_email_body(
    missing_invoices: pd.DataFrame,
    cancelled_invoices: pd.DataFrame,
    paid_invoices: pd.DataFrame,
    failed_invoice_updates: pd.DataFrame,
    folio_url: str,
) -> str:
    def _generate_folio_url(uuid: str) -> str:
        invoice_url = f"{folio_url}/invoice/view/{uuid}"
        return f"""<a href="{invoice_url}">Invoice {uuid}</a>"""

    def _update_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = dataframe.replace({np.nan: None})
        if "invoice_id" in dataframe.columns:
            dataframe["Invoice URL"] = dataframe["invoice_id"].apply(
                _generate_folio_url
            )
            dataframe = dataframe.drop(columns=["invoice_id"])
        return dataframe

    cancelled_invoices = _update_dataframe(cancelled_invoices)
    paid_invoices = _update_dataframe(paid_invoices)
    failed_invoice_updates = _update_dataframe(failed_invoice_updates)

    template = Template(
        """
        <h1>Invoices Failures from AP Report</h1>
        {% if missing|length > 0 %}
        <h2>Missing Invoices</h2>
        {{ missing.to_html(escape=False, index=False)|safe }}
        {% endif %}
        {% if cancelled|length > 0 %}
        <h2>Cancelled Invoices</h2>
        {{ cancelled.to_html(escape=False, index=False)|safe }}
        {% endif %}
        {% if paid|length > 0 %}
        <h2>Already Paid Invoices</h2>
        {{ paid.to_html(escape=False, index=False)|safe }}
        {% endif %}
        {% if update_errors|length > 0 %}
        <h2>Error Updating Invoices</h2>
        {{ update_errors.to_html(escape=False, index=False)|safe }}
        {% endif %}
        """
    )

    return template.render(
        missing=missing_invoices,
        cancelled=cancelled_invoices,
        paid=paid_invoices,
        update_errors=failed_invoice_updates,
    )


def _invoice_line_links(invoice: Invoice, folio_url) -> str:
    template = Template(
        """
        <ul>
            {% for line in lines %}
             <li><a href="{{ line.url }}">Invoice line number: {{ line.number }}</a></li>
            {% endfor %}
        </ul>
        """
    )
    lines = []
    for i, line in enumerate(invoice.lines):
        if (
            any(
                [
                    fund_dist.distributionType == "amount"
                    for fund_dist in line.fundDistributions
                ]
            )
            or line.subTotal == 0
        ):
            lines.append(
                {
                    "url": f"{folio_url}/invoice/view/{invoice.id}/line/{line.id}/view",
                    "number": i,
                }
            )
    return template.render(lines=lines)


def _ap_report_paid_email_body(
    invoices: list, ap_report_name: str, folio_url: str
) -> str:
    if len(invoices) < 1:
        return ""

    template = Template(
        """
        <h2>Paid Invoices</h2>
        <p>From ap report {{ ap_report_name }}</p>
        <ul>
        {% for invoice in invoices %}
        <li>
            <a href="{{ folio_url}}/invoice/view/{{invoice.id}}">Vendor Invoice Number: {{ invoice.vendorInvoiceNo }}</a>
        </li>
        {% endfor %}
        </ul>
        """
    )
    return template.render(
        ap_report_name=ap_report_name, invoices=invoices, folio_url=folio_url
    )


def _excluded_email_body(grouped_reasons: dict, folio_url: str) -> str:
    jinja_env = Environment()  # noqa
    jinja_env.filters["invoice_line_links"] = _invoice_line_links

    template = jinja_env.from_string(
        """
        {% for reason, invoices in grouped_reasons.items() %}
        <h2>{{ reason }}</h2>
        <ol>
          {% for invoice in invoices %}
          {% if reason == "Amount split" or reason == "Zero subtotal" %}
          <li>
            Vendor Invoice Number: {{ invoice.vendorInvoiceNo }}
            {{ invoice|invoice_line_links(folio_url) }}
          </li>
          {% else %}
          <li>
             <a href="{{ folio_url}}/invoice/view/{{invoice.id }}">Vendor Invoice Number: {{ invoice.vendorInvoiceNo }}</a>
          </li>
          {% endif %}
          {% endfor %}
        </ol>
        {% endfor %}
        """
    )
    return template.render(grouped_reasons=grouped_reasons, folio_url=folio_url)


def _summary_email_body(invoices: list, folio_url: str):
    if len(invoices) < 1:
        return ""

    converter = models_converter()
    jinja_env = Environment(autoescape=True)

    template = jinja_env.from_string(
        """
    <h2>Approved Invoices Sent to AP</h2>
    <ol>
      {% for invoice in invoices %}
       <li>
            <a href="{{ folio_url}}/invoice/view/{{invoice.id }}">Vendor Invoice Number: {{ invoice.vendorInvoiceNo }}</a>
       </li>
      {% endfor %}
    </ol>
    """
    )
    invoice_instances = [converter.structure(invoice, Invoice) for invoice in invoices]
    return template.render(invoices=invoice_instances, folio_url=folio_url)


def _group_excluded_invoices(invoices_reasons: list):
    """
    Groups excluded invoices by acq unit and exclusion reason
    """
    grouped_acqunits: dict = {}
    converter = models_converter()
    for row in invoices_reasons:
        acq_unit = row["invoice"]["acqUnitIds"][0]
        if acq_unit in grouped_acqunits:
            if row["reason"] in grouped_acqunits[acq_unit]:
                grouped_acqunits[acq_unit][row["reason"]].append(
                    converter.structure(row["invoice"], Invoice)
                )
            else:
                grouped_acqunits[acq_unit][row["reason"]] = [
                    converter.structure(row["invoice"], Invoice)
                ]
        else:
            grouped_acqunits[acq_unit] = {
                row["reason"]: [converter.structure(row["invoice"], Invoice)]
            }
    return grouped_acqunits


def _group_invoices_by_acqunit(invoices: list) -> dict:
    """
    Groups invoices by acq unit ID
    """
    grouped_acqunits: dict = {}
    for row in invoices:
        acq_unit = row["acqUnitIds"][0]
        if acq_unit in grouped_acqunits:
            grouped_acqunits[acq_unit].append(row)
        else:
            grouped_acqunits[acq_unit] = [row]
    return grouped_acqunits


def generate_excluded_email(invoices_reasons: list):
    """
    Generates emails for excluded invoices
    """
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")
    bus_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_BUS")
    folio_url = Variable.get("FOLIO_URL")

    grouped_invoices = _group_excluded_invoices(invoices_reasons)
    bus_invoices = grouped_invoices.get("c74ceb20-33fb-4b50-914e-a056db67feea", {})
    law_invoices = grouped_invoices.get("556eb26f-dbea-41c1-a1de-9a88ad950d95", {})
    sul_invoices = grouped_invoices.get("bd6c5f05-9ab3-41f7-8361-1c1e847196d3", {})

    sul_html_content = _excluded_email_body(sul_invoices, folio_url)
    if len(sul_html_content.strip()) > 0:
        logger.info(f"Sending email to {sul_to_email_addr} for SUL rejected invoices")
        send_email_with_server_name(
            to=[
                sul_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Rejected Invoices for SUL",
            html_content=sul_html_content,
        )

    bus_html_content = _excluded_email_body(bus_invoices, folio_url)

    if len(bus_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {bus_to_email_addr} for Business rejected invoices"
        )
        send_email_with_server_name(
            to=[bus_to_email_addr, devs_to_email_addr],
            subject="Rejected Invoices for Business",
            html_content=bus_html_content,
        )

    law_html_content = _excluded_email_body(law_invoices, folio_url)
    if len(law_html_content.strip()) > 0:
        logger.info(f"Sending email to {law_to_email_addr} for Law rejected invoices")
        send_email_with_server_name(
            to=[
                law_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Rejected Invoices for LAW",
            html_content=law_html_content,
        )


def generate_failed_dag_email(context, airflow_url=None):
    """
    Sends email when ap_payment_report DAG fails
    """
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    dag_run = context.get('dag_run')

    dag_url = dag_run_url(dag_run=dag_run, airflow_url=airflow_url)

    html_content = f"""<h1>ap_payment_report DAG Failed</h1>
    <p>DAG Run: <a href="{dag_url}">{dag_run.run_id}</a></p>
    """

    send_email_with_server_name(
        to=[devs_to_email_addr],
        subject="Failed ap_payment_report DAG",
        html_content=html_content,
    )


def generate_ap_error_report_email(
    missing: list, cancelled: list, already_paid: list, failed_updates: list
) -> bool:
    """
    Gets lists of missing/cancelled/paid/failed updates errors and emails report
    """
    logger.info("Generating Email Report")
    missing_invoices_df = pd.DataFrame(missing)
    cancelled_invoices_df = pd.DataFrame(cancelled)
    paid_invoices_df = pd.DataFrame(already_paid)
    failed_updates_df = pd.DataFrame(failed_updates)

    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")
    bus_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_BUS")
    folio_url = Variable.get("FOLIO_URL")

    logger.info(
        f"Sending email to {sul_to_email_addr}, {bus_to_email_addr}, and {law_to_email_addr} error reports"
    )

    html_content = _ap_report_errors_email_body(
        missing_invoices_df,
        cancelled_invoices_df,
        paid_invoices_df,
        failed_updates_df,
        folio_url,
    )

    try:
        send_email_with_server_name(
            to=[
                sul_to_email_addr,
                law_to_email_addr,
                bus_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Invoice Errors from AP Report",
            html_content=html_content,
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def generate_voucher_error_email(
    missing: list, multiples: list, failed_updates: list
) -> bool:
    """
    Gets lists of missing/multiple/failed updates errors and emails report
    Lists contain invoice_id's and failed_updates is list of voucher_to_update
    """
    logger.info("Generating Email Report")
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")
    bus_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_BUS")
    folio_url = Variable.get("FOLIO_URL")

    logger.info(
        f"Sending email to {sul_to_email_addr}, {bus_to_email_addr}, and {law_to_email_addr} error reports"
    )

    template = Template(
        """
        <h1>Voucher Failures from AP Report</h1>
        {% if missing|length > 0 %}
        <h2>Missing Vouchers for Invoice</h2>
        <ul>
        {% for invoice_id in missing %}
        <li>
              <a href="{{ folio_url}}/invoice/view/{{invoice_id}}">Invoice {{invoice_id}}</a>.
        </li>
        {% endfor %}
        </ul>
        {% endif %}
        {% if multiples|length > 0 %}
        <h2>Multiple Vouchers Found for Invoice</h2>
        <ul>
        {% for invoice_id in multiples %}
        <li>
              <a href="{{ folio_url}}/invoice/view/{{invoice_id}}">Invoice {{invoice_id}}</a>.
        </li>
        {% endfor %}
        </ul>
        {% endif %}
        {% if failed_updates|length > 0 %}
        <h2>Failed to Update Voucher for Invoice</h2>
        <ul>
        {% for voucher in failed_updates %}
        <li>
              <a href="{{ folio_url}}/invoice/view/{{voucher.invoiceId}}/voucher/{{voucher.id}}/view">Voucher {{voucher.id}}</a>.
        </li>
        {% endfor %}
        </ul>
        {% endif %}
        """
    )

    html_content = template.render(
        missing=missing,
        multiples=multiples,
        failed_updates=failed_updates,
        folio_url=folio_url,
    )

    try:
        send_email_with_server_name(
            to=[
                sul_to_email_addr,
                law_to_email_addr,
                bus_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Voucher Errors from AP Report",
            html_content=html_content,
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def generate_ap_paid_report_email(invoices: list, report_path: str) -> dict:
    """
    Generates emails for Paid Invoices and Vouchers from AP Report
    invoices = [{folio_invoice_data}, {folio_invoice_data}]
    Returns a dictionary for succeeded/failed to send email, e.g.: {"sul": True, "law": True, "bus": True}
    """
    ap_report_name = pathlib.Path(report_path).name
    grouped_invoices = _group_invoices_by_acqunit(invoices)
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")
    bus_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_BUS")
    folio_url = Variable.get("FOLIO_URL")

    bus_invoices = grouped_invoices.get("c74ceb20-33fb-4b50-914e-a056db67feea", [])
    law_invoices = grouped_invoices.get("556eb26f-dbea-41c1-a1de-9a88ad950d95", [])
    sul_invoices = grouped_invoices.get("bd6c5f05-9ab3-41f7-8361-1c1e847196d3", [])

    sul_html_content = _ap_report_paid_email_body(
        sul_invoices,
        ap_report_name,
        folio_url,
    )

    email_sent: dict = {}

    if len(sul_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {sul_to_email_addr} for {len(sul_invoices):,} invoices"
        )
        try:
            send_email_with_server_name(
                to=[sul_to_email_addr, devs_to_email_addr],
                subject=f"Paid Invoices from {ap_report_name} for SUL",
                html_content=sul_html_content,
            )
            email_sent["sul"] = True
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            email_sent["sul"] = False

    business_html_content = _ap_report_paid_email_body(
        bus_invoices, ap_report_name, folio_url
    )

    if len(business_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {bus_to_email_addr} for {len(bus_invoices):,} invoices"
        )
        try:
            send_email_with_server_name(
                to=[bus_to_email_addr, devs_to_email_addr],
                subject=f"Paid Invoices from {ap_report_name} for Business",
                html_content=business_html_content,
            )
            email_sent["bus"] = True
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            email_sent["bus"] = False

    law_html_content = _ap_report_paid_email_body(
        law_invoices,
        ap_report_name,
        folio_url,
    )
    if len(law_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {law_to_email_addr} for {len(law_invoices):,} invoices"
        )
        try:
            send_email_with_server_name(
                to=[law_to_email_addr, devs_to_email_addr],
                subject=f"Paid Invoices from {ap_report_name} for LAW",
                html_content=law_html_content,
            )
            email_sent["law"] = True
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            email_sent["law"] = False

    return email_sent


def generate_summary_email(invoices: list):
    """
    Generates emails that summarize invoices sent to AP
    """
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")
    bus_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_BUS")
    folio_url = Variable.get("FOLIO_URL")

    grouped_invoices = _group_invoices_by_acqunit(invoices)

    bus_invoices = grouped_invoices.get("c74ceb20-33fb-4b50-914e-a056db67feea", [])
    law_invoices = grouped_invoices.get("556eb26f-dbea-41c1-a1de-9a88ad950d95", [])
    sul_invoices = grouped_invoices.get("bd6c5f05-9ab3-41f7-8361-1c1e847196d3", [])

    sul_html_content = _summary_email_body(sul_invoices, folio_url)
    if len(sul_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {sul_to_email_addr} for {len(sul_invoices)} invoices"
        )
        send_email_with_server_name(
            to=[
                sul_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Approved Invoices Sent to AP for SUL",
            html_content=sul_html_content,
        )

    business_html_content = _summary_email_body(bus_invoices, folio_url)

    if len(business_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {bus_to_email_addr} for {len(bus_invoices):,} invoices"
        )
        send_email_with_server_name(
            to=[bus_to_email_addr, devs_to_email_addr],
            subject="Approved Invoices Sent to AP for Business",
            html_content=business_html_content,
        )

    law_html_content = _summary_email_body(law_invoices, folio_url)

    if len(law_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {law_to_email_addr} for {len(law_invoices):,} invoices"
        )
        send_email_with_server_name(
            to=[
                law_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Approved Invoices Sent to AP for LAW",
            html_content=law_html_content,
        )
