from datetime import datetime
import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


log = logging.getLogger(__name__)

SOURCE_CONN_ID = "PostgreSQL_Source"
DW_CONN_ID = "PostgreSQL_DW"
SOURCE_SCHEMA = "bookings"
RAW_SCHEMA = "raw"
TABLES = [
    "bookings",
    "tickets",
    "ticket_flights",
    "boarding_passes",
    "airports",
    "flights",
    "aircrafts",
    "seats",
]


def _build_create_table_ddl(columns, table_name):
    column_defs = []
    for name, data_type, is_nullable in columns:
        nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
        column_defs.append(f"{name} {data_type} {nullable}")
    cols_sql = ", ".join(column_defs)
    return f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{table_name} ({cols_sql})"


def _introspect_columns(source_hook, table_name):
    sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    return source_hook.get_records(sql, parameters=(SOURCE_SCHEMA, table_name))


def _sync_table(table_name):
    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    dw = PostgresHook(postgres_conn_id=DW_CONN_ID)

    dw.run(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

    columns = _introspect_columns(source, table_name)
    if not columns:
        raise ValueError(f"Nenhuma coluna encontrada para tabela {table_name} em {SOURCE_SCHEMA}")

    dw.run(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{table_name}")
    ddl = _build_create_table_ddl(columns, table_name)
    dw.run(ddl)

    source_conn = source.get_conn()
    dw_conn = dw.get_conn()
    source_cursor = source_conn.cursor()
    column_names = [c[0] for c in columns]
    cols_sql = ", ".join(column_names)
    source_cursor.execute(
        f"SELECT {cols_sql} FROM {SOURCE_SCHEMA}.{table_name}"
    )
    dw_cursor = dw_conn.cursor()
    insert_sql = f"INSERT INTO {RAW_SCHEMA}.{table_name} ({cols_sql}) VALUES %s"

    total_rows = 0
    while True:
        rows = source_cursor.fetchmany(10000)
        if not rows:
            break
        execute_values(dw_cursor, insert_sql, rows)
        dw_conn.commit()
        batch_count = len(rows)
        total_rows += batch_count
        log.info("Inseridos %s registros em raw.%s", batch_count, table_name)

    log.info("Carga completa da tabela raw.%s: %s registros inseridos", table_name, total_rows)

    source_cursor.close()
    dw_cursor.close()
    source_conn.close()
    dw_conn.close()


@dag(
    dag_id="raw_bookings_layer",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data_engineering"},
    tags=["raw", "bookings", "postgres"],
)
def raw_bookings_layer():
    @task
    def sync_table_task(table_name: str):
        _sync_table(table_name)

    for table in TABLES:
        sync_table_task.override(task_id=f"sync_{table}")(table)


dag = raw_bookings_layer()
