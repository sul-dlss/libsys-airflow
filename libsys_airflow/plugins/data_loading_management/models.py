from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Text
from sqlalchemy.orm import declarative_base, relationship, Session
from airflow.providers.postgres.hooks.postgres import PostgresHook

Model = declarative_base()

class Organization(Model):
    __tablename__ = "organization"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    uuid = Column(String(36), unique=True, nullable=False)
    last_folio_update = Column(DateTime, nullable=False)

    def __repr__(self) -> str:
        return f"{self.name} - {self.uuid}"


class Interface(Model):
    __tablename__ = "interface"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    uuid = Column(String(36), unique=True, nullable=False)
    file_regex = Column(String(35))
    job_profile_uuid = Column(String(36), nullable=False)
    last_folio_update = Column(DateTime, nullable=False)
    vendor_id = Column(Integer, ForeignKey("organization.id"))
    vendor = relationship("plugins.data_loading_management.models.Organization")

    def __repr__(self) -> str:
        return f"{self.name} - {self.uuid}"


class JobStatus(Model):
    __tablename__ = "job_status"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)

    def __repr__(self) -> str:
        return self.name


class JobRun(Model):
    __tablename__ = "job_run"

    id = Column(Integer, primary_key=True)
    dag_run_id = Column(String(40), nullable=False)
    update = Column(DateTime, nullable=False)
    interface_id = Column(Integer, ForeignKey("interface.id"))
    interface = relationship("plugins.data_loading_management.models.Interface")
    status_id = Column(Integer, ForeignKey("job_status.id"))
    status = relationship("plugins.data_loading_management.models.JobStatus")

    def __repr__(self) -> str:
        return self.dag_run_id
    

class File(Model):
    __tablename__ = "file"

    id = Column(Integer, primary_key=True)
    name = Column(String(40), nullable=False)
    path = Column(Text, nullable=False)
    size = Column(Integer, nullable=False)
    md5 = Column(String(36), nullable=False)
    job_run_id = Column(Integer, ForeignKey("job_run.id"))
    job_run = relationship("plugins.data_loading_management.models.JobRun")

    def __repr__(self) -> str:
        return self.name
    

pg_hook = PostgresHook("dataloading_app")

Model.metadata.create_all(pg_hook.get_sqlalchemy_engine())
