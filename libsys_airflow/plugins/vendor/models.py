from datetime import datetime
import enum
import re

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
from sqlalchemy.sql.expression import true
from typing import List, Any

Model = declarative_base()


class Vendor(Model):
    __tablename__ = "vendors"

    id = Column(Integer, primary_key=True)
    display_name = Column(String(120), unique=False, nullable=False)
    folio_organization_uuid = Column(String(36), unique=True, nullable=False)
    vendor_code_from_folio = Column(String(36), unique=True, nullable=False)
    acquisitions_unit_from_folio = Column(String(36), unique=False, nullable=True)
    acquisitions_unit_name_from_folio = Column(String(36), unique=False, nullable=True)
    last_folio_update = Column(DateTime, nullable=False)
    vendor_interfaces = relationship(
        "VendorInterface",
        back_populates="vendor",
        order_by='VendorInterface.id',
        lazy="joined",
    )

    def __repr__(self) -> str:
        return f"{self.display_name} - {self.folio_organization_uuid}"

    @property
    def active_vendor_interfaces(self) -> List['VendorInterface']:
        return list(
            [interface for interface in self.vendor_interfaces if interface.active]
        )

    @classmethod
    def with_active_vendor_interfaces(cls, session: Session) -> List['Vendor']:
        return session.scalars(
            select(cls)
            .join(VendorInterface)
            .filter(VendorInterface.active == true())
            .distinct()
            .order_by(Vendor.display_name)
        ).unique()

    @classmethod
    def with_vendor_interfaces(cls, session: Session) -> List['Vendor']:
        return session.scalars(
            select(cls).join(VendorInterface).distinct().order_by(Vendor.display_name)
        ).unique()


class VendorInterface(Model):
    __tablename__ = "vendor_interfaces"

    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey("vendors.id"))
    vendor = relationship("Vendor", back_populates="vendor_interfaces")
    display_name = Column(String(50), unique=False, nullable=False)
    # A null folio_interface_uuid indicates that upload only.
    folio_interface_uuid = Column(String(36), unique=False, nullable=True)
    folio_data_import_profile_uuid = Column(String(36), unique=False, nullable=True)
    folio_data_import_processing_name = Column(String(50), unique=False, nullable=True)
    file_pattern = Column(String(250), unique=False, nullable=True)
    remote_path = Column(String(250), unique=False, nullable=True)
    processing_dag = Column(String(50), unique=False, nullable=True)
    processing_options = Column(JSON, nullable=True)
    processing_delay_in_days = Column(Integer, unique=False, nullable=True)
    active = Column(Boolean, nullable=False, default=False)
    # Vendor interface is currently assigned to organization within FOLIO. Upload-only are False
    assigned_in_folio = Column(Boolean, nullable=False, default=True)
    vendor_files = relationship("VendorFile", back_populates="vendor_interface")

    @property
    def pending_files(self):
        """Returns a list of VendorFile objects that are not_fetched, fetching_error, fetched, uploaded, or loading."""
        session = Session.object_session(self)
        return session.scalars(
            select(VendorFile)
            .filter(VendorFile.vendor_interface_id == self.id)
            .filter(
                VendorFile.status.in_(
                    [
                        FileStatus.not_fetched,
                        FileStatus.fetching_error,
                        FileStatus.fetched,
                        FileStatus.loading,
                        FileStatus.uploaded,
                    ]
                )
            )
            .order_by(VendorFile.created.desc())
        ).all()

    @property
    def processed_files(self):
        """Returns a list of VendorFile objects that are loaded or loading_error."""
        session = Session.object_session(self)
        return session.scalars(
            select(VendorFile)
            .filter(VendorFile.vendor_interface_id == self.id)
            .filter(
                VendorFile.status.in_([FileStatus.loaded, FileStatus.loading_error])
            )
            .order_by(VendorFile.loaded_timestamp.desc())
        ).all()

    @property
    def interface_uuid(self) -> str:
        # This accounts for upload only interfaces, which don't have a folio_interface_uuid.
        return self.folio_interface_uuid or f"upload_only-{self.id}"

    def __repr__(self) -> str:
        return f"{self.display_name} - {self.interface_uuid}"

    def processing_option(self, key: str, default: Any = None) -> Any:
        if self.processing_options is not None:
            return self.processing_options.get(key, default)
        else:
            return default

    @property
    def package_name(self):
        return self.processing_option('package_name')

    @property
    def delete_marc(self):
        return self.processing_option('delete_marc', [])

    @property
    def change_marc(self):
        return self.processing_option('change_marc', [])

    @property
    def archive_regex(self):
        return self.processing_option('archive_regex')

    @property
    def upload_only(self):
        return self.folio_interface_uuid is None

    @classmethod
    def load(cls, interface_uuid: str, session: Session) -> 'VendorInterface':
        match = re.match(r'^upload_only-(\d+)$', interface_uuid)
        if match:
            id = int(match.group(1))
            return session.get(cls, id)
        else:
            return session.scalars(
                select(cls).where(cls.folio_interface_uuid == interface_uuid)
            ).first()


class FileStatus(enum.Enum):
    not_fetched = "not_fetched"
    fetching_error = "fetching_error"
    fetched = "fetched"
    uploaded = "uploaded"
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
    # Name of the file after it has been processed.
    processed_filename = Column(String(250), unique=False, nullable=True)
    filesize = Column(Integer, nullable=False)
    vendor_timestamp = Column(DateTime, nullable=True)
    loaded_timestamp = Column(DateTime, nullable=True)
    expected_load_time = Column(DateTime, nullable=True)
    archive_date = Column(Date, nullable=True)
    status = Column(
        Enum(FileStatus),
        nullable=False,
        default=FileStatus.not_fetched,
        server_default=FileStatus.not_fetched.value,
    )
    dag_run_id = Column(String(350), unique=True, nullable=True)
    folio_job_execution_uuid = Column(String(36), unique=False, nullable=True)

    def __repr__(self) -> str:
        return f"{self.vendor_filename} - {self.vendor_timestamp}"

    @classmethod
    def load(cls, interface_uuid: str, filename: str, session: Session) -> 'VendorFile':
        vendor_interface = VendorInterface.load(interface_uuid, session)
        return cls.load_with_vendor_interface(vendor_interface, filename, session)

    @classmethod
    def load_with_vendor_interface(
        cls, vendor_interface: VendorInterface, filename: str, session: Session
    ) -> 'VendorFile':
        return session.scalars(
            select(cls)
            .where(cls.vendor_interface_id == vendor_interface.id)
            .where(cls.vendor_filename == filename)
        ).first()

    @classmethod
    def ready_for_data_import(cls, session: Session) -> List["VendorFile"]:
        """
        Returns a list of VendorFile objects that are ready for loading into
        Folio. These are files that have a status of "fetched" and which have an
        expected_load_time in the past. Results are ordered in ascending order
        of when they were fetched, and no more than 1000 are returned at a time.
        """
        return session.scalars(
            select(VendorFile)
            .filter(
                VendorFile.status.in_([FileStatus.fetched]),
            )
            # not really needed but explicitly ignore files not assigned a load time
            .filter(VendorFile.expected_load_time.is_not(None))
            .filter(VendorFile.expected_load_time <= datetime.utcnow())
            .limit(1000)
            .order_by(VendorFile.created.asc())
        ).all()
