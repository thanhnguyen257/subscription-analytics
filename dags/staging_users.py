from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
# Define the absolute path to the users.csv
path = "/data/master_db/users.csv"
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
    CONN_ID = config["conn_id"]
    @task
    def store_users():        
        print(config)

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.copy_expert(
            sql="""
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
                FROM STDIN WITH CSV HEADER""",
            filename=path
        )
    store_users()
create_users()