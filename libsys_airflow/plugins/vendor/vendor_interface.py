from sqlalchemy.orm import Session
from sqlalchemy import select
import re

from libsys_airflow.plugins.vendor.models import VendorInterface


def load_vendor_interface(interface_uuid: str, session: Session) -> VendorInterface:
    match = re.match(r'^upload_only-(\d+)$', interface_uuid)
    if match:
        id = int(match.group(1))
        return session.get(VendorInterface, id)
    else:
        return session.scalars(
            select(VendorInterface).where(
                VendorInterface.folio_interface_uuid == interface_uuid
            )
        ).first()
