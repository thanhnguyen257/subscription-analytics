from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
# Define the absolute path to the users.csv
path = "/data/master_db/licenses.csv"
@dag(
    dag_id="create_licenses",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)
def create_licenses():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    CONN_ID = config["conn_id"]
    @task
    def store_licenses():        
        print(config)

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.copy_expert(
            sql="""
                COPY license_keys (
                    license_id,
                    subscription_id,
                    max_seats,
                    issued_date,
                    expiry_date
                )
                FROM STDIN WITH CSV HEADER""",
            filename=path
        )
    store_licenses()
create_licenses()