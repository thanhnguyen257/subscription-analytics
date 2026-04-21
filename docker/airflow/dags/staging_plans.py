from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

@dag(
    dag_id="create_subscription_plans",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)
def create_plans():

    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")

    POSTGRES_CONN_ID = config["postgres_conn_id"]
    BLOB_CONN_ID = "blob_storage"
    CONTAINER_NAME = "raw"
    BLOB_FILE_NAME = "staging_plans.sql"

    @task
    def create_plans_sql():
        blob_hook = WasbHook(wasb_conn_id=BLOB_CONN_ID)

        sql_content = blob_hook.read_file(
            container_name=CONTAINER_NAME,
            blob_name=BLOB_FILE_NAME,
        )

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(sql_content)
        conn.commit()

        cursor.close()
        conn.close()

    create_plans_sql()

create_plans()