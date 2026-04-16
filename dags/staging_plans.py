from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
from airflow.models import Variable
# Define the absolute path to the users.csv
path = "/opt/airflow/data/master_db/plans.csv"
@dag(
    dag_id="create_subscription_plans",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)
# product_id,product_name,category,created_at
def create_plans():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    CONN_ID = config["conn_id"]
    STAGING_DB = config["databases"]["staging"]
    create_plans_sql = SQLExecuteQueryOperator(
        task_id="create_plans",
        conn_id=CONN_ID,
        database=STAGING_DB,
        sql='sql/staging_plans.sql'
    )
        
    create_plans_sql
create_plans()