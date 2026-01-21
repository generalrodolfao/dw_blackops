import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values, Json

# --- Configuration ---
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

log = logging.getLogger(__name__)

# --- Helper Functions for Raw Layer ---
def _map_data_type(data_type: str) -> str:
    dt = data_type.lower()
    if dt in ("character varying", "character", "text"):
        return "text"
    if dt in ("json", "jsonb"):
        return "jsonb"
    if dt.startswith("timestamp"):
        return "timestamp with time zone"
    if dt == "numeric":
        return "numeric"
    if dt in ("integer", "int4"):
        return "integer"
    if dt in ("bigint", "int8"):
        return "bigint"
    return data_type

def _build_create_table_ddl(columns, table_name):
    column_defs = []
    for name, data_type, is_nullable in columns:
        mapped_type = _map_data_type(data_type)
        nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
        column_defs.append(f"{name} {mapped_type} {nullable}")
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
    """
    Extracts data from Source and Loads into Raw (Full Refresh).
    """
    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    dw = PostgresHook(postgres_conn_id=DW_CONN_ID)

    # Ensure Schema Exists (Idempotent)
    dw.run(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

    # 1. Get Source Schema
    columns = _introspect_columns(source, table_name)
    if not columns:
        raise ValueError(f"No columns found for {table_name} in {SOURCE_SCHEMA}")

    # 2. Recreate Destination Table
    dw.run(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{table_name}")
    ddl = _build_create_table_ddl(columns, table_name)
    dw.run(ddl)

    # 3. Extract & Load
    source_conn = source.get_conn()
    dw_conn = dw.get_conn()
    source_cursor = source_conn.cursor()
    
    column_names = [c[0] for c in columns]
    cols_sql = ", ".join(column_names)
    
    # Use server-side cursor or just fetchmany for memory safety
    source_cursor.execute(f"SELECT {cols_sql} FROM {SOURCE_SCHEMA}.{table_name}")
    
    dw_cursor = dw_conn.cursor()
    insert_sql = f"INSERT INTO {RAW_SCHEMA}.{table_name} ({cols_sql}) VALUES %s"

    total_rows = 0
    
    # Identify JSON columns for special handling
    json_indices = [
        idx for idx, (_, data_type, _) in enumerate(columns)
        if data_type.lower() in ("json", "jsonb")
    ]

    while True:
        rows = source_cursor.fetchmany(10000)
        if not rows:
            break

        # Normalize rows to handle dicts and JSON serialization
        # Check first row to detect if driver returns dicts or tuples
        first = rows[0]
        normalized_rows = []
        
        if isinstance(first, dict):
            for row in rows:
                values = []
                for idx, col_name in enumerate(column_names):
                    val = row[col_name]
                    if idx in json_indices and val is not None:
                        val = Json(val)
                    values.append(val)
                normalized_rows.append(tuple(values))
        else:
            for row in rows:
                values = list(row)
                for idx in json_indices:
                    val = values[idx]
                    if val is not None:
                        values[idx] = Json(val)
                normalized_rows.append(tuple(values))

        execute_values(dw_cursor, insert_sql, normalized_rows)
        dw_conn.commit()
        
        batch_count = len(normalized_rows)
        total_rows += batch_count
        log.info(f"Inserted {batch_count} rows into {RAW_SCHEMA}.{table_name}")

    log.info(f"Finished syncing {table_name}: {total_rows} total rows.")
    
    source_cursor.close()
    dw_cursor.close()
    source_conn.close()
    dw_conn.close()

# --- DAG Definition ---
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'aviation_etl_pipeline',
    default_args=default_args,
    description='Unified Pipeline: Source -> Raw -> Staging -> Mart',
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql'],
    tags=['aviation', 'etl', 'consolidated']
) as dag:

    # 1. Setup Schemas
    setup_schemas = PostgresOperator(
        task_id='setup_schemas',
        postgres_conn_id=DW_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS staging; CREATE SCHEMA IF NOT EXISTS mart;"
    )

    # 2. Raw Layer (Parallel Extraction)
    # Using PythonOperator via @task decorator style for loop
    raw_tasks = []
    for table in TABLES:
        @task(task_id=f"sync_{table}")
        def sync_task(t: str):
            _sync_table(t)
        
        raw_op = sync_task(table)
        setup_schemas >> raw_op
        raw_tasks.append(raw_op)

    # 3. Staging Layer (Transformation)
    # Consolidates Raw data into clean Staging tables
    staging_layer = PostgresOperator(
        task_id='staging_layer',
        postgres_conn_id=DW_CONN_ID,
        sql='staging/create_staging_tables.sql'
    )

    # 4. Mart Layer - Dimensions
    dim_date = PostgresOperator(
        task_id='dim_date',
        postgres_conn_id=DW_CONN_ID,
        sql='mart/01_dim_date.sql'
    )

    load_dimensions = PostgresOperator(
        task_id='load_dimensions',
        postgres_conn_id=DW_CONN_ID,
        sql='mart/02_load_dimensions.sql'
    )

    # 5. Mart Layer - Fact
    load_fact = PostgresOperator(
        task_id='load_fact',
        postgres_conn_id=DW_CONN_ID,
        sql='mart/03_load_fact.sql'
    )

    # --- Dependencies ---
    # All raw tasks must finish before Staging starts
    raw_tasks >> staging_layer
    
    # Staging must finish before Dimensions
    staging_layer >> load_dimensions
    
    # Dim Date is independent of Staging (can run in parallel with raw/staging)
    setup_schemas >> dim_date
    
    # Fact table requires all Dimensions to be ready
    [dim_date, load_dimensions] >> load_fact
