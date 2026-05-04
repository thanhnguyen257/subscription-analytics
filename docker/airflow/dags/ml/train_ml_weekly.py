from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="train_ml_weekly",
    start_date=datetime(2026, 4, 1),
    schedule="@weekly",
    catchup=False,
    tags=["ml", "weekly"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10)
    }
) as dag:

    run_train_ml_weekly = DatabricksRunNowOperator(
        task_id="run_train_ml_weekly",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("train_ml_weekly_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )