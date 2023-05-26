import os
from sqlalchemy.orm import scoped_session, sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook

# See https://flask.palletsprojects.com/en/2.3.x/patterns/sqlalchemy/


# The EngineWrapper allows for late creation of the PG connection.
# This allows for mocking out Session in testing.
class EngineWrapper:
    def __init__(self):
        self.engine = None

    def __getattr__(self, name):
        if self.engine is None:
            self.engine = PostgresHook("vendor_loads").get_sqlalchemy_engine(
                max_overflow=int(os.getenv("DATABASE_MAX_OVERFLOW", "10"))
            )
        return getattr(self.engine, name)


Session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=EngineWrapper())
)
