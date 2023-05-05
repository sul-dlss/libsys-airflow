from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Text, Boolean
from sqlalchemy.orm import declarative_base, relationship, Session

Model = declarative_base()


class Vendor(Model):
    __tablename__ = "vendors"

    id = Column(Integer, primary_key=True)
    display_name = Column(String(50), unique=True, nullable=False)
    folio_organization_uuid = Column(String(36), unique=True, nullable=False)
    vendor_code_from_folio = Column(String(36), unique=True, nullable=False)
    acquisitions_unit_from_folio = Column(String(36), unique=False, nullable=False)
    has_active_vendor_interfaces = Column(Boolean, nullable=False, default=False)
    last_folio_update = Column(DateTime, nullable=False)

    def __repr__(self) -> str:
        return f"{self.display_name} - {self.folio_organization_uuid}"


class VendorInterface(Model):
    __tablename__ = "vendor_interfaces"

    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey('vendors.id'))
    display_name = Column(String(50), unique=False, nullable=False)
    folio_interface_uuid = Column(String(36), unique=True, nullable=False)
    folio_data_import_profile_uuid = Column(String(36), unique=True, nullable=True)
    folio_data_import_processing_name = Column(String(50), unique=False, nullable=True)
    file_pattern = Column(String(250), unique=False, nullable=True)
    remote_path = Column(String(250), unique=False, nullable=True)
    processing_dag = Column(String(50), unique=False, nullable=True)
    processing_delay_in_days = Column(Integer, unique=False, nullable=True)
    active = Column(Boolean, nullable=False, default=False)

    def __repr__(self) -> str:
        return f"{self.display_name} - {self.folio_interface_uuid}"
