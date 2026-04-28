from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
from airflow.models import Variable
# Define the absolute path to the users.csv
# path = "/data/master_db/plans.csv"
@dag(
    dag_id="create_and_insert_dump_data_v1",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)
# product_id,product_name,category,created_at
def create_and_insert_dump():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    CONN_ID = config["conn_id"]
    STAGING_DB = config["databases"]["dump"]
    create_dump_sql = SQLExecuteQueryOperator(
        task_id="create_dump",
        conn_id=CONN_ID,
        database=STAGING_DB,
        sql='sql/dump.sql'
    )
        
    create_dump_sql
create_and_insert_dump()