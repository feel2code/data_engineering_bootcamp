import logging

import pandas as pd
import pendulum
import vertica_python
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

vertica_conn_info = {
    "host": Variable.get("VERTICA_HOST"),
    "port": Variable.get("VERTICA_PORT"),
    "user": Variable.get("VERTICA_USER"),
    "password": Variable.get("VERTICA_PASSWORD"),
    "database": Variable.get("VERTICA_DB"),
    "autocommit": True,
}
postgres_hook = PostgresHook("postgresql_conn")
engine = postgres_hook.get_sqlalchemy_engine()


def load_from_postgres_and_convert_to_df(pg_query, params=None) -> pd.DataFrame:
    """Load data from Postgres"""
    with engine.connect() as pg_conn:
        return pd.read_sql_query(sql=pg_query, con=pg_conn, params=params)


def vertica_execute(query, formatter, query_type) -> None:
    """Execute query in Vertica"""
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            if query_type == "execute":
                cur.execute(query, formatter)
            if query_type == "copy":
                cur.copy(query, formatter)


def log_message(error_msg, e):
    """Log error message and raise the error"""
    log.error(f"{error_msg} {str(e)}")
    raise ValueError(error_msg) from e


def format_date(dt):
    """Format date via pendulum lib"""
    return pendulum.from_format(dt, "YYYY-MM-DD").to_date_string()
