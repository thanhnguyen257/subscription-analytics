from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="bronze_usage_events_hourly",
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    tags=["bronze", "hourly"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3)
    }
) as dag:

    run_usage_events = DatabricksRunNowOperator(
        task_id="run_bronze_usage_events",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("bronze_hourly_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )