from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
from airflow.models import Variable
# Define the absolute path to the users.csv
path = "/opt/airflow/data/master_db/products.csv"
@dag(
    dag_id="create_products",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)

def create_products():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    CONN_ID = config["conn_id"]
    STAGING_DB = config["databases"]["staging"]
    create_products_sql = SQLExecuteQueryOperator(
        task_id="create_products",
        conn_id=CONN_ID,
        database=STAGING_DB,
        sql='sql/staging_products.sql'
    )
        
    create_products_sql
create_products()