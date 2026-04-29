from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.exceptions import AirflowFailException
from datetime import datetime
import re

POSTGRES_CONN_ID = "postgres"
BLOB_CONN_ID = "blob_storage"
CONTAINER_NAME = "mock-project-source"

def fix_timestamp(sql):
    # add quotes around datetime patterns
    return re.sub(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        r"'\1'",
        sql
    )

@dag(
    dag_id="staging_seed_ingestion_weekly",
    schedule="@weekly",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["staging"]
)
def seed_ingestion():

    @task
    def ensure_tables():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        pg.run("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT,
            product_name TEXT,
            category TEXT,
            created_at TIMESTAMP,
            dw_ingested_at TIMESTAMP,
            dw_source_file TEXT,
            dw_batch_id TEXT
        );

        CREATE TABLE IF NOT EXISTS subscription_plans (
            plan_id INT,
            product_id INT,
            tier TEXT,
            billing_cycle TEXT,
            price DECIMAL(10,2),
            currency TEXT,
            created_at TIMESTAMP,
            dw_ingested_at TIMESTAMP,
            dw_source_file TEXT,
            dw_batch_id TEXT
        );
        """)

    @task
    def load_sql_from_blob():
        blob = WasbHook(wasb_conn_id=BLOB_CONN_ID)
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        batch_id = "{{ ts_nodash }}"

        files = [
            "raw-files/products/products.sql",
            "raw-files/subscription_plans/subscription_plans.sql"
        ]

        conn = pg.get_conn()
        cursor = conn.cursor()

        for file_path in files:
            try:
                sql_content = blob.read_file(
                    container_name=CONTAINER_NAME,
                    blob_name=file_path
                )

                sql_content = fix_timestamp(sql_content)

                # Wrap with metadata update if needed
                wrapped_sql = f"""
                BEGIN;

                {sql_content}

                -- Add metadata after insert
                UPDATE {file_path.split('/')[-1].replace('.sql','')}
                SET 
                    dw_ingested_at = NOW(),
                    dw_source_file = '{file_path}',
                    dw_batch_id = '{batch_id}';

                COMMIT;
                """

                cursor.execute(wrapped_sql)
                conn.commit()

            except Exception as e:
                conn.rollback()
                raise AirflowFailException(f"Failed loading {file_path}: {str(e)}")

        cursor.close()
        conn.close()

    ensure_tables() >> load_sql_from_blob()

seed_ingestion()