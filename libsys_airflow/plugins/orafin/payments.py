import logging

from datetime import datetime

from airflow.utils.email import send_email
from cattrs import Converter

from libsys_airflow.plugins.folio.folio_client import FolioClient
from libsys_airflow.plugins.orafin.models import Invoice, FeederFile

logger = logging.getLogger(__name__)


def _convert_ts_class(timestamp, cls):
    return cls.fromisoformat(timestamp)


def generate_excluded_email(invoices: list, folio_url: str):
    """
    Generates emails for excluded invoices
    """
    converter = models_converter()
    html_content = """<h2>Amount Split</h2><ul>"""
    for invoice_dict in invoices:
        invoice = converter.structure(invoice_dict, Invoice)
        html_content += f"""<li>
        Vendor Invoice Number: {invoice.internal_number}
        {_invoice_line_links(invoice, folio_url)}</li>"""
    html_content += "</ul>"
    send_email(
        # to=["sa-payments@lists.stanford.edu"],
        to=["jpnelson@stanford.edu"],
        subject="Rejected Invoices for SUL",
        html_content=html_content,
    )


def _get_fund(fund_distributions: list, folio_client: FolioClient):
    """
    Retrieves funds from fund distribution lines
    """
    for distribution in fund_distributions:
        fund_id = distribution['fundId']
        fund_result = folio_client.get(f"/finance/funds/{fund_id}")
        distribution["fund"] = {
            "id": fund_id,
            "externalAccountNo": fund_result["fund"].get("externalAccountNo"),
        }


def _get_invoice_lines(invoice_id: str, folio_client: FolioClient) -> tuple:
    invoice_lines_result = folio_client.get(
        "/invoice/invoice-lines",
        params={"query": f"invoiceId=={invoice_id}", "limit": 250},
    )
    invoice_lines = invoice_lines_result.get("invoiceLines", [])
    exclude_invoice = False
    for row in invoice_lines:
        if "poLineId" in row:
            po_line_id = row['poLineId']
            po_line = {"id": po_line_id}
            po_result = folio_client.get(f"/orders/order-lines/{po_line_id}")
            po_line["acquisitionMethod"] = po_result["acquisitionMethod"]
            po_line["orderFormat"] = po_result.get("orderFormat")
            if "eresource" in po_result:
                po_line["materialType"] = po_result["eresource"].get("materialType")
            elif "physical" in po_result:
                po_line["materialType"] = po_result["physical"].get("materialType")
            row["poLine"] = po_line
        fund_distributions = row.get("fundDistributions", [])
        if any(
            [
                fund_dist["distributionType"] == "amount"
                for fund_dist in fund_distributions
            ]
        ):
            exclude_invoice = True
        _get_fund(fund_distributions, folio_client)
    return invoice_lines, exclude_invoice


def get_invoice(
    invoice_id: str, folio_client: FolioClient, converter: Converter
) -> tuple:
    """
    Retrieves Invoice, Invoice Lines, and Vendor
    """
    # Retrieves Invoice Details
    invoice = folio_client.get(f"/invoice/invoices/{invoice_id}")
    # Retrieves Invoices Lines and Purchase Order
    invoice["lines"], exclude_invoice = _get_invoice_lines(invoice_id, folio_client)
    # Call to Okapi organization endpoint to see VAT is applicable
    invoice["vendor"] = folio_client.get(
        f"/organizations/organizations/{invoice['vendorId']}"
    )
    # Converts to Invoice Object
    invoice = converter.structure(invoice, Invoice)
    return invoice, exclude_invoice


def _invoice_line_links(invoice: Invoice, folio_url) -> str:
    html_output = "<ol>"
    for line in invoice.lines:
        if any(
            [
                fund_dist.distributionType == "amount"
                for fund_dist in line.fundDistributions
            ]
        ):
            line_url = f"{folio_url}/invoice/invoice-lines/{line.id}"
            html_output = f""""<li><a href="{line_url}">{line.id}</a></li>"""
    html_output += "</ol>"
    return html_output


def init_feeder_file(
    invoices: list, folio_client: FolioClient, converter: Converter
) -> FeederFile:
    invoice_models = []
    for invoice_dict in invoices:
        invoice = converter.structure(invoice_dict, Invoice)
        invoice_models.append(invoice)
    feeder_file = FeederFile(invoices=invoice_models)
    feeder_file.add_expense_lines(folio_client)
    return feeder_file


def models_converter():
    converter = Converter()
    converter.register_structure_hook(datetime, _convert_ts_class)
    converter.register_unstructure_hook(datetime, datetime.isoformat)
    return converter
