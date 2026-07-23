import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from libsys_airflow.plugins.shared.folio_client import folio_client
from folioclient import FolioError

logger = logging.getLogger(__name__)


def add_admin_note_to_record(note: str, record_type: str, id: str) -> bool:
    date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d")
    logger.info(f"Adding admin note {note} with date {date} to {record_type}")
    client = folio_client()
    match record_type:
        case "instance":
            endpoint = f"/inventory/instances/{id}"
        case "holdings":
            endpoint = f"/holdings-storage/holdings/{id}"
        case "item":
            endpoint = f"/inventory/items/{id}"

    record = client.folio_get(endpoint)
    admin_note = record["administrativeNotes"]
    note_updated = False
    for index, item in enumerate(admin_note):
        if item.startswith(note):
            admin_note[index] = f"{note}/{date}"
            note_updated = True
            break
    if not note_updated:
        record["administrativeNotes"].append(f"{note}/{date}")
    try:
        client.folio_put(endpoint, record)
        return True
    except FolioError as e:
        logger.error(f"Adding admin note failed: {e}")
        return False
