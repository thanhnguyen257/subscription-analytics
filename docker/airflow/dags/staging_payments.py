from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

path = "/opt/airflow/data/master_db/payments.csv"
@dag(
    dag_id="create_payments",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)
def create_payments():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    CONN_ID = config["conn_id"]
    @task
    def store_payments():        
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.copy_expert(
            sql="""
                COPY payments (
                    payment_id,
                    subscription_id,
                    amount,
                    currency,
                    payment_status,
                    payment_date,
                    payment_method
                )
                FROM STDIN WITH CSV HEADER""",
            filename=path
        )

    store_payments()
create_payments()