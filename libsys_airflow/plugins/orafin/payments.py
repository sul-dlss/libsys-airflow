import logging
import pathlib

from datetime import datetime, timezone

from cattrs import Converter

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

from folioclient import FolioClient

from libsys_airflow.plugins.folio.finances import current_fiscal_years, active_ledgers
from libsys_airflow.plugins.orafin.models import Invoice, FeederFile
from libsys_airflow.plugins.shared.utils import is_production

logger = logging.getLogger(__name__)


def _convert_ts_class(timestamp, cls):
    return cls.fromisoformat(timestamp)


def _get_fund(fund_distributions: list, folio_client: FolioClient):
    """
    Retrieves funds from fund distribution lines
    """
    for distribution in fund_distributions:
        fund_id = distribution['fundId']
        fund_result = folio_client.folio_get(f"/finance/funds/{fund_id}")
        distribution["fund"] = {
            "id": fund_id,
            "externalAccountNo": fund_result["fund"].get("externalAccountNo"),
        }


def _get_invoice_lines(invoice_id: str, folio_client: FolioClient) -> tuple:
    invoice_lines_result = folio_client.folio_get(
        "/invoice/invoice-lines",
        query_params={
            "query": f"invoiceId=={invoice_id} sortBy metadata.createdDate invoiceLineNumber",
            "limit": 250,
        },
    )
    invoice_lines = invoice_lines_result.get("invoiceLines", [])
    exclude_invoice = False
    exclusion_reason = ""
    for row in invoice_lines:
        if "poLineId" in row:
            po_line_id = row['poLineId']
            po_line = {"id": po_line_id}
            po_result = folio_client.folio_get(
                f"/orders/order-lines/{po_line_id}", query_params={"limit": 250}
            )
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
            exclusion_reason = "Amount split"
        if row["subTotal"] == 0.0:
            exclude_invoice = True
            exclusion_reason = "Zero subtotal"
        _get_fund(fund_distributions, folio_client)
    return invoice_lines, exclude_invoice, exclusion_reason


def generate_file(feeder_file: dict, folio_client: FolioClient) -> FeederFile:
    converter = models_converter()

    feeder_file_instance = converter.structure(feeder_file, FeederFile)

    feeder_file_instance.add_expense_lines(folio_client)

    return feeder_file_instance


def get_invoice(
    invoice_id: str, folio_client: FolioClient, converter: Converter
) -> tuple:
    """
    Retrieves Invoice, Invoice Lines, and Vendor
    """
    # Retrieves Invoice Details
    invoice = folio_client.folio_get(f"/invoice/invoices/{invoice_id}")
    # Retrieves Invoices Lines and Purchase Order
    invoice["lines"], exclude_invoice, exclusion_reason = _get_invoice_lines(
        invoice_id, folio_client
    )
    # Call to Okapi organization endpoint to see VAT is applicable
    invoice["vendor"] = folio_client.folio_get(
        f"/organizations/organizations/{invoice['vendorId']}"
    )
    # Converts to Invoice Object
    invoice = converter.structure(invoice, Invoice)
    # Check for invoice-level exclusions
    if invoice.fiscalYearId not in current_fiscal_years(
        active_ledgers(folio_client), folio_client
    ):
        exclude_invoice = True
        exclusion_reason = "Fiscal year not current"
    if invoice.invoiceDate > datetime.now(timezone.utc):
        exclude_invoice = True
        exclusion_reason = "Future invoice date"
    if "FEEDER" not in invoice.accountingCode:
        exclude_invoice = True
        exclusion_reason = "Not FEEDER vendor"
    if len(f"{invoice.vendorInvoiceNo} {invoice.folioInvoiceNo}") > 40:
        exclude_invoice = True
        exclusion_reason = "Invoice number too long"
    return invoice, exclude_invoice, exclusion_reason


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


def transfer_to_orafin(feeder_file: str):
    if is_production():
        server_info = "of_aplib@intxfer-prd.stanford.edu:/home/of_aplib/OF1_PRD"
    else:
        server_info = "of_aplib@intxfer-uat.stanford.edu:/home/of_aplib/OF1_DEV"
    command = [
        "scp",
        "-i /opt/airflow/vendor-keys/apserver.key",
        "-o StrictHostKeyChecking=no",
        str(feeder_file),
        f"{server_info}/inbound/data/xxdl_ap_lib.dat",
    ]
    logger.info(f"Command {command}")
    return BashOperator(task_id="sftp_feederfile", bash_command=" ".join(command))


# We should switch to using this function when the AP server is upgraded
def hook_transfer_to_orafin(feeder_file_path: pathlib.Path, sftp_connection: str):
    sftp_hook = SFTPHook(sftp_connection)

    with feeder_file_path.open() as fo:
        sftp_hook.store_file(
            "/home/of_aplib/OF1_PRD/inbound/data/xxdl_ap_lib.dat", fo.read()
        )
    logger.info(f"Uploaded {feeder_file_path.name} with {sftp_connection} connection")
