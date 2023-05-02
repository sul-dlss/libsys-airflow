from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Text, Boolean
from sqlalchemy.orm import declarative_base, relationship, Session
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
