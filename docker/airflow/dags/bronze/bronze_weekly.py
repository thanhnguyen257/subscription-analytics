from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="bronze_weekly",
    start_date=datetime(2026, 4, 1),
    schedule="@weekly",
    catchup=False,
    tags=["bronze", "weekly"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10)
    }
) as dag:

    run_bronze_weekly = DatabricksRunNowOperator(
        task_id="run_bronze_weekly",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("bronze_weekly_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )