from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
# Define the absolute path to the users.csv
path = "/data/master_db/subscriptions.csv"
@dag(
    dag_id="create_subscriptions",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=['staging']
)
def create_subscriptions():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    CONN_ID = config["conn_id"]
    @task
    def store_subscriptions():        
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.copy_expert(
            sql="""
                COPY subscriptions (
                    subscription_id,
                    user_id,
                    plan_id,
                    start_date,
                    end_date,
                    status,
                    current_mrr,
                    created_at
                )
                FROM STDIN WITH CSV HEADER""",
            filename=path
        )
    store_subscriptions()
create_subscriptions()