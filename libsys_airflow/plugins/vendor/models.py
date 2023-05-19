import enum
from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    select,
)
from sqlalchemy.orm import declarative_base, relationship, Session


Model = declarative_base()


class Vendor(Model):
    __tablename__ = "vendors"

    id = Column(Integer, primary_key=True)
    display_name = Column(String(120), unique=True, nullable=False)
    folio_organization_uuid = Column(String(36), unique=True, nullable=False)
    vendor_code_from_folio = Column(String(36), unique=True, nullable=False)
    acquisitions_unit_from_folio = Column(String(36), unique=False, nullable=False)
    has_active_vendor_interfaces = Column(Boolean, nullable=False, default=False)
    last_folio_update = Column(DateTime, nullable=False)
    vendor_interfaces = relationship("VendorInterface", back_populates="vendor")

    def __repr__(self) -> str:
        return f"{self.display_name} - {self.folio_organization_uuid}"


class VendorInterface(Model):
    __tablename__ = "vendor_interfaces"

    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey("vendors.id"))
    vendor = relationship("Vendor", back_populates="vendor_interfaces")
    display_name = Column(String(50), unique=False, nullable=False)
    folio_interface_uuid = Column(String(36), unique=True, nullable=False)
    folio_data_import_profile_uuid = Column(String(36), unique=False, nullable=True)
    folio_data_import_processing_name = Column(String(50), unique=False, nullable=True)
    file_pattern = Column(String(250), unique=False, nullable=True)
    remote_path = Column(String(250), unique=False, nullable=True)
    processing_dag = Column(String(50), unique=False, nullable=True)
    processing_options = Column(JSON, nullable=True)
    processing_delay_in_days = Column(Integer, unique=False, nullable=True)
    active = Column(Boolean, nullable=False, default=False)
    vendor_files = relationship("VendorFile", back_populates="vendor_interface")

    @property
    def pending_files(self):
        """Returns a list of VendorFile objects that have not been loaded yet."""
        session = Session.object_session(self)
        return session.scalars(
            select(VendorFile)
            .filter(VendorFile.vendor_interface_id == self.id)
            .filter(VendorFile.dag_run_id.is_(None))
            .order_by(VendorFile.created.desc())
        ).all()

    @property
    def processed_files(self):
        """Returns a list of VendorFile objects that have been loaded yet."""
        session = Session.object_session(self)
        return session.scalars(
            select(VendorFile)
            .filter(VendorFile.vendor_interface_id == self.id)
            .filter(VendorFile.dag_run_id.is_not(None))
            .order_by(VendorFile.loaded_timestamp.desc())
        ).all()

    def __repr__(self) -> str:
        return f"{self.display_name} - {self.folio_interface_uuid}"


class FileStatus(enum.Enum):
    not_fetched = "not_fetched"
    fetching_error = "fetching_error"
    fetched = "fetched"
    loading = "loading"
    loading_error = "loading_error"
    loaded = "loaded"
    purged = "purged"
    skipped = "skipped"  # Files on the FTP server that are not fetched because they are before the download window.


class VendorFile(Model):
    __tablename__ = "vendor_files"

    id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False)
    updated = Column(DateTime, nullable=False)
    vendor_interface_id = Column(Integer, ForeignKey("vendor_interfaces.id"))
    vendor_interface = relationship("VendorInterface", back_populates="vendor_files")
    vendor_filename = Column(String(250), unique=False, nullable=False)
    filesize = Column(Integer, nullable=False)
    vendor_timestamp = Column(DateTime, nullable=True)
    loaded_timestamp = Column(DateTime, nullable=True)
    expected_execution = Column(Date, nullable=False)
    status = Column(
        Enum(FileStatus),
        nullable=False,
        default=FileStatus.not_fetched,
        server_default=FileStatus.not_fetched.value,
    )
    dag_run_id = Column(String(350), unique=True, nullable=True)

    def __repr__(self) -> str:
        return f"{self.vendor_filename} - {self.vendor_timestamp}"
