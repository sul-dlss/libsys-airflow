import logging
import pathlib

from jinja2 import Environment, Template

from airflow.models import Variable
from airflow.utils.email import send_email

from libsys_airflow.plugins.orafin.models import Invoice
from libsys_airflow.plugins.orafin.payments import models_converter

logger = logging.getLogger(__name__)


def _ap_report_errors_email_body(
    missing_invoices: list,
    cancelled_invoices: list,
    paid_invoices: list,
    folio_url: str,
) -> str:
    template = Template(
        """
        <h1>Invoices Failures from AP Report</h1>
        {% if missing|length > 0 %}
        <h2>Missing Invoices</h2>
        <ul>
        {% for folio_invoice_number in missing %}
        <li>{{ folio_invoice_number }} found in AP Report but not in FOLIO</li>
        {% endfor %}
        </ul>
        {% endif %}
        {% if cancelled|length > 0 %}
        <h2>Cancelled Invoices</h2>
        <ul>
        {% for invoice_id in cancelled %}
        <li>
            <a href="{{ folio_url}}/invoice/view/{{invoice_id }}">Invoice {{invoice_id }}</a>
        </li>
        {% endfor %}
        </ul>
        {% endif %}
        {% if paid|length > 0 %}
        <h2>Already Paid Invoices</h2>
        <ul>
        {% for invoice_id in paid %}
        <li>
          <a href="{{ folio_url}}/invoice/view/{{invoice_id }}">Invoice {{invoice_id }}</a>
        </li>
        {% endfor %}
        </ul>
        {% endif %}
        """
    )

    return template.render(
        missing=missing_invoices,
        cancelled=cancelled_invoices,
        paid=paid_invoices,
        folio_url=folio_url,
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
    jinja_env = Environment()
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
    jinja_env = Environment()

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


def _group_invoices_by_acqunit(invoices: list):
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


def generate_excluded_email(invoices_reasons: list, folio_url: str):
    """
    Generates emails for excluded invoices
    """
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")

    grouped_invoices = _group_excluded_invoices(invoices_reasons)

    sul_html_content = _excluded_email_body(
        grouped_invoices.get("bd6c5f05-9ab3-41f7-8361-1c1e847196d3", {}), folio_url
    )
    if len(sul_html_content.strip()) > 0:
        logger.info(f"Sending email to {sul_to_email_addr} for rejected invoices")
        send_email(
            to=[
                sul_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Rejected Invoices for SUL",
            html_content=sul_html_content,
        )

    law_html_content = _excluded_email_body(
        grouped_invoices.get("556eb26f-dbea-41c1-a1de-9a88ad950d95", {}), folio_url
    )
    if len(law_html_content.strip()) > 0:
        logger.info(f"Sending email to {law_to_email_addr} for rejected invoices")
        send_email(
            to=[
                law_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Rejected Invoices for LAW",
            html_content=law_html_content,
        )


def generate_invoice_error_email(invoice_id: str, folio_url: str, ti=None):
    """
    Retrieves AP report information for invoice that failed to update and
    emails report
    """
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")
    ap_report_row = ti.xcom_pull(task_ids="retrieve_invoice_task", key=invoice_id)

    template = Template(
        """<h1>Error Updating Invoice</h1>
        <p>
            Failed to update <a href="{{ folio_url}}/invoice/view/{{invoice_id}}">{{invoice_id}}</a>.
        </p>
        From AP Report
        <table>
          <tr>
            <th>Amount Paid</th>
            <th>Payment Number</th>
            <th>Payment Date</th>
          </tr>
          <tr>
            <td>{{row["AmountPaid"]}}</td>
            <td>{{row["PaymentNumber"]}}</td>
            <td>{{row["PaymentDate"]}}</td>
          </tr>
        </table>
    """
    )

    html_content = template.render(
        folio_url=folio_url, invoice_id=invoice_id, row=ap_report_row
    )

    send_email(
        to=[
            sul_to_email_addr,
            law_to_email_addr,
            devs_to_email_addr,
        ],
        subject=f"Error Updating Invoice {invoice_id}",
        html_content=html_content,
    )


def generate_ap_error_report_email(folio_url: str, ti=None) -> int:
    """
    Retrieves Errors from upstream tasks and emails report
    """
    task_instance = ti
    logger.info("Generating Email Report")
    missing_invoices = task_instance.xcom_pull(
        task_ids='retrieve_invoice_task', key='missing'
    )
    if missing_invoices is None:
        missing_invoices = []
    logger.info(f"Missing {len(missing_invoices):,}")
    cancelled_invoices = task_instance.xcom_pull(
        task_ids='retrieve_invoice_task', key='cancelled'
    )
    if cancelled_invoices is None:
        cancelled_invoices = []
    logger.info(f"Cancelled {len(cancelled_invoices):,}")
    paid_invoices = task_instance.xcom_pull(
        task_ids='retrieve_invoice_task', key='paid'
    )
    if paid_invoices is None:
        paid_invoices = []
    logger.info(f"Paid {len(paid_invoices):,}")
    total_errors = len(missing_invoices) + len(cancelled_invoices) + len(paid_invoices)

    # No errors, don't send email
    if total_errors < 1:
        return total_errors

    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")

    logger.info(
        f"Sending email to {sul_to_email_addr} and {law_to_email_addr} for {total_errors} error reports"
    )

    html_content = _ap_report_errors_email_body(
        missing_invoices, cancelled_invoices, paid_invoices, folio_url
    )

    send_email(
        to=[
            sul_to_email_addr,
            law_to_email_addr,
            devs_to_email_addr,
        ],
        subject="Invoice Errors from AP Report",
        html_content=html_content,
    )
    return total_errors


def generate_ap_paid_report_email(folio_url: str, task_instance=None):
    """
    Generates emails for Paid Invoices and Vouchers from AP Report
    """
    ap_report_path = task_instance.xcom_pull(task_ids="init_processing_task")
    invoices = task_instance.xcom_pull(task_ids="retrieve_invoice_task")
    ap_report_name = pathlib.Path(ap_report_path).name
    grouped_invoices = _group_invoices_by_acqunit(invoices)
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")

    sul_html_content = _ap_report_paid_email_body(
        grouped_invoices.get("bd6c5f05-9ab3-41f7-8361-1c1e847196d3", []),
        ap_report_name,
        folio_url,
    )

    if len(sul_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {sul_to_email_addr} for {len(grouped_invoices.get('bd6c5f05-9ab3-41f7-8361-1c1e847196d3', [])):,} invoices"
        )
        send_email(
            to=[
                sul_to_email_addr,
                devs_to_email_addr,
            ],
            subject=f"Paid Invoices from {ap_report_name} for SUL",
            html_content=sul_html_content,
        )

    law_html_content = _ap_report_paid_email_body(
        grouped_invoices.get("556eb26f-dbea-41c1-a1de-9a88ad950d95", []),
        ap_report_name,
        folio_url,
    )
    if len(law_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {law_to_email_addr} for {len(grouped_invoices.get('556eb26f-dbea-41c1-a1de-9a88ad950d95', [])):,} invoices"
        )
        send_email(
            to=[
                law_to_email_addr,
                devs_to_email_addr,
            ],
            subject=f"Paid Invoices from {ap_report_name} for LAW",
            html_content=law_html_content,
        )

    return len(invoices)


def generate_summary_email(invoices: list, folio_url: str):
    """
    Generates emails that summarize invoices sent to AP
    """
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")

    grouped_invoices = _group_invoices_by_acqunit(invoices)

    sul_html_content = _summary_email_body(
        grouped_invoices.get("bd6c5f05-9ab3-41f7-8361-1c1e847196d3", []), folio_url
    )
    if len(sul_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {sul_to_email_addr} for {len(grouped_invoices.get('bd6c5f05-9ab3-41f7-8361-1c1e847196d3', [])):,} invoices"
        )
        send_email(
            to=[
                sul_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Approved Invoices Sent to AP for SUL",
            html_content=sul_html_content,
        )

    law_html_content = _summary_email_body(
        grouped_invoices.get("556eb26f-dbea-41c1-a1de-9a88ad950d95", []), folio_url
    )
    if len(law_html_content.strip()) > 0:
        logger.info(
            f"Sending email to {law_to_email_addr} for {len(grouped_invoices.get('556eb26f-dbea-41c1-a1de-9a88ad950d95', [])):,} invoices"
        )
        send_email(
            to=[
                law_to_email_addr,
                devs_to_email_addr,
            ],
            subject="Approved Invoices Sent to AP for LAW",
            html_content=law_html_content,
        )
