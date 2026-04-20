from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import io

@dag(
    dag_id="create_users",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)
def create_users():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    POSTGRES_CONN_ID = config["postgres_conn_id"]
    BLOB_CONN_ID = "blob_storage"
    CONTAINER_NAME = "raw"
    BLOB_FILE_NAME = "users.csv"
    @task
    def store_users():
        blob_hook = WasbHook(wasb_conn_id=BLOB_CONN_ID)

        csv_content = blob_hook.read_file(
            container_name=CONTAINER_NAME,
            blob_name=BLOB_FILE_NAME,
        )

        csv_buffer = io.StringIO(csv_content)
    
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
    
        copy_sql="""
                COPY users (
                    user_id,
                    first_name,
                    last_name,
                    email,
                    country,
                    age,
                    gender,
                    acquisition_channel,
                    is_enterprise,
                    created_at
                )
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
        """

        cursor.copy_expert(copy_sql, csv_buffer)
        conn.commit()

        cursor.close()
        conn.close()

    store_users()
create_users()