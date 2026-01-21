from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'aviation_dw_pipeline',
    default_args=default_args,
    description='End-to-end Aviation DW Pipeline (Staging -> Mart)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql']
) as dag:

    # 1. Setup Schemas
    setup_schemas = PostgresOperator(
        task_id='setup_schemas',
        postgres_conn_id='PostgreSQL_DW',
        sql="CREATE SCHEMA IF NOT EXISTS staging; CREATE SCHEMA IF NOT EXISTS mart;"
    )

    # 2. Staging Layer (Parallel Execution potential, but bundled in one SQL for simplicity here)
    # Reads from RAW, Writes to STAGING
    staging_layer = PostgresOperator(
        task_id='staging_layer',
        postgres_conn_id='PostgreSQL_DW',
        sql='staging/create_staging_tables.sql'
    )

    # 3. Mart Layer - Dimensions
    # Dim Date (Independent)
    dim_date = PostgresOperator(
        task_id='dim_date',
        postgres_conn_id='PostgreSQL_DW',
        sql='mart/01_dim_date.sql'
    )

    # Common Dimensions (Airports, Aircrafts, Passengers) - Depend on Staging
    load_dimensions = PostgresOperator(
        task_id='load_dimensions',
        postgres_conn_id='PostgreSQL_DW',
        sql='mart/02_load_dimensions.sql'
    )

    # 4. Mart Layer - Fact
    # Concatenated Fact Table
    load_fact = PostgresOperator(
        task_id='load_fact',
        postgres_conn_id='PostgreSQL_DW',
        sql='mart/03_load_fact.sql'
    )

    # Dependencies
    setup_schemas >> staging_layer
    setup_schemas >> dim_date
    
    staging_layer >> load_dimensions
    
    [dim_date, load_dimensions] >> load_fact
