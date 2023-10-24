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
        if any(
            [
                fund_dist.distributionType == "amount"
                for fund_dist in line.fundDistributions
            ]
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
    template = Template(
        """
        <h2>Paid Invoices</h2>
        <p>From ap report {{ ap_report_name }}</p>
        <ul>
        {% for invoice_id in invoices %}
        <li>
            <a href="{{ folio_url}}/invoice/view/{{invoice_id }}">Invoice {{invoice_id }}</a>
        </li>
        {% endfor %}
        </ul>
        """
    )
    return template.render(
        ap_report_name=ap_report_name, invoices=invoices, folio_url=folio_url
    )


def _excluded_email_body(invoices: list, folio_url: str) -> str:
    converter = models_converter()
    jinja_env = Environment()
    jinja_env.filters["invoice_line_links"] = _invoice_line_links

    template = jinja_env.from_string(
        """
    <h2>Amount Split</h2>
    <ol>
      {% for invoice in invoices %}
       <li>
            Vendor Invoice Number: {{ invoice.vendorInvoiceNo }}
            {{ invoice|invoice_line_links(folio_url) }}
       </li>
      {% endfor %}
    </ol>
    """
    )
    invoice_instances = [converter.structure(invoice, Invoice) for invoice in invoices]
    return template.render(invoices=invoice_instances, folio_url=folio_url)


def generate_excluded_email(invoices: list, folio_url: str):
    """
    Generates emails for excluded invoices
    """
    to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")

    html_content = _excluded_email_body(invoices, folio_url)
    logger.info(f"Sending email to {to_email_addr} for {len(invoices):,} invoices")
    send_email(
        to=[
            to_email_addr,
        ],
        subject="Rejected Invoices for SUL",
        html_content=html_content,
    )


def _summary_email_body(invoices: list, folio_url: str):
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

    to_email_addr = Variable.get("ORAFIN_TO_EMAIL")

    logger.info(f"Sending email to {to_email_addr} for {total_errors} error reports")

    html_content = _ap_report_errors_email_body(
        missing_invoices, cancelled_invoices, paid_invoices, folio_url
    )

    send_email(
        to=[
            to_email_addr,
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
    html_content = _ap_report_paid_email_body(invoices, ap_report_name, folio_url)
    to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    send_email(
        to=[
            to_email_addr,
        ],
        subject=f"Paid Invoices from {ap_report_name}",
        html_content=html_content,
    )
    return len(invoices)


def generate_summary_email(invoices: list, folio_url: str):
    """
    Generates emails that summarize invoices sent to AP
    """
    to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")

    html_content = _summary_email_body(invoices, folio_url)
    logger.info(f"Sending email to {to_email_addr} for {len(invoices):,} invoices")
    send_email(
        to=[
            to_email_addr,
        ],
        subject="Approved Invoices Sent to AP for SUL",
        html_content=html_content,
    )
