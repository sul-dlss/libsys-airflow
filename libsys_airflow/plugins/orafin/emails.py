import logging

from jinja2 import Environment, Template

from airflow.models import Variable
from airflow.utils.email import send_email

from libsys_airflow.plugins.orafin.models import Invoice
from libsys_airflow.plugins.orafin.payments import models_converter

logger = logging.getLogger(__name__)


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
