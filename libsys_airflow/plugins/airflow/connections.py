from airflow.models import Connection
from sqlalchemy.orm.session import Session
from airflow.utils.session import NEW_SESSION, provide_session


@provide_session
def find_or_create_conn(conn_type, host, login, pwd, session: Session = NEW_SESSION):
    conn_id = f'{conn_type}-{host}'
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host,
                      login=login)
    conn.set_password(pwd)
    existing_conn = session.query(conn.__class__).filter_by(conn_id=conn.conn_id).first()

    if existing_conn:
        session.delete(existing_conn)
        session.commit()
    session.add(conn)
    session.commit()

    return conn_id
