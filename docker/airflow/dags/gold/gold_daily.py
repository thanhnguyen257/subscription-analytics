from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="gold_daily",
    start_date=datetime(2026, 4, 1),
    schedule=None,  # IMPORTANT
    catchup=False,
    tags=["gold"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    run_gold = DatabricksRunNowOperator(
        task_id="run_gold_aggregation",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("gold_job_id")),
        notebook_params={
            "batch_id": "{{ dag_run.conf.get('batch_id') }}"
        }
    )