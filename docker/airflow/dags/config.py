from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime

@dag(
    dag_id="config",
    start_date=datetime(2026, 4, 1),
    schedule=None,   # run manually only when needed
    catchup=False,
    tags=["staging"]
)
def set_variable():
    @task
    def save_config():
        Variable.set(
            "dag_config",
            {
                "postgres_conn_id": "postgres",
                "azure_sql_conn_id": "azure_sql",
                "databases": "postgres"
            },
            serialize_json=True
        )
        print("Saved the Variables")
    save_config()
set_variable()