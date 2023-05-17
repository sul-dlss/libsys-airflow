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
    Text,
)
from sqlalchemy.orm import declarative_base, relationship


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

    def __repr__(self) -> str:
        return f"{self.vendor_filename} - {self.vendor_timestamp}"


class DataLoadStatus(enum.Enum):
    not_loaded = "not_loaded"
    loading_error = "loading_error"
    loaded = "loaded"


class FileDataLoad(Model):
    __tablename__ = "file_data_loads"

    id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False)
    updated = Column(DateTime, nullable=False)
    vendor_file_id = Column(Integer, ForeignKey("vendor_files.id"))
    dag_run_id = Column(String(36), unique=True, nullable=False)
    dag_run_starttime = Column(DateTime, nullable=True)
    dag_run_endtime = Column(DateTime, nullable=True)
    status = Column(
        Enum(DataLoadStatus),
        nullable=False,
        default=DataLoadStatus.not_loaded,
        server_default=DataLoadStatus.not_loaded.value,
    )
    additional_info = Column(Text, nullable=True)
    folio_job_execution_uuid = Column(String(36), unique=False, nullable=True)

    def __repr__(self) -> str:
        return f"{self.id} - {self.vendor_file_id} - {self.dag_run_id}"
