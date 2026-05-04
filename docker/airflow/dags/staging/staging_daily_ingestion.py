from airflow.sdk import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

POSTGRES_CONN_ID = "postgres"
AZURE_SQL_CONN_ID = "azure_sql"
BATCH_SIZE = 5000


@dag(
    dag_id="staging_daily_ingestion",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["staging", "databricks"]
)
def staging_daily_ingestion():

    @task
    def ensure_tables():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        pg.run("""
        CREATE TABLE IF NOT EXISTS license_keys (
            license_id BIGINT,
            subscription_id BIGINT,
            max_seats INT,
            issued_date DATE,
            expiry_date DATE,
            created_at TIMESTAMP,
            dw_ingested_at TIMESTAMP,
            dw_source_file TEXT,
            dw_batch_id TEXT
        );

        CREATE TABLE IF NOT EXISTS license_allocations (
            allocation_id BIGINT,
            license_id BIGINT,
            seat_number INT,
            status TEXT,
            allocation_date DATE,
            created_at TIMESTAMP,
            dw_ingested_at TIMESTAMP,
            dw_source_file TEXT,
            dw_batch_id TEXT
        );
        """)

    def load_table(table_name, pk_column):
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        azure = OdbcHook(
            odbc_conn_id=AZURE_SQL_CONN_ID,
            driver="ODBC Driver 18 for SQL Server",
            database="mock-project-azure-sql"
        )

        batch_id = "{{ ts_nodash }}"

        last_watermark = pg.get_first(f"""
            SELECT COALESCE(MAX(created_at), '1900-01-01')
            FROM {table_name}
        """)[0]

        offset = 0

        while True:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE created_at >= '{last_watermark}'
                ORDER BY created_at, {pk_column}
                OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY
            """

            rows = azure.get_records(query)

            if not rows:
                break

            enriched = [
                tuple(list(r) + [
                    datetime.now(),
                    f"azure_sql.{table_name}",
                    batch_id
                ])
                for r in rows
            ]

            pg.insert_rows(
                table=table_name,
                rows=enriched,
                target_fields=[
                    "license_id","subscription_id","max_seats",
                    "issued_date","expiry_date","created_at",
                    "dw_ingested_at","dw_source_file","dw_batch_id"
                ] if table_name == "license_keys" else [
                    "allocation_id","license_id","seat_number",
                    "status","allocation_date","created_at",
                    "dw_ingested_at","dw_source_file","dw_batch_id"
                ],
                commit_every=1000
            )

            offset += BATCH_SIZE

    @task
    def load_license_keys():
        load_table("license_keys", "license_id")

    @task
    def load_license_allocations():
        load_table("license_allocations", "allocation_id")


    run_csv_to_adls = DatabricksRunNowOperator(
        task_id="run_csv_to_adls",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("staging_pipline_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )

    join_ingestion = EmptyOperator(task_id="join_ingestion")

    ensure_tables() >> [load_license_keys(), load_license_allocations()]
    [load_license_keys(), load_license_allocations()] >> run_csv_to_adls
    run_csv_to_adls >> join_ingestion


staging_daily_ingestion()