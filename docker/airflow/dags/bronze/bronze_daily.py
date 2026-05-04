from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="bronze_daily",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "daily"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    run_bronze_daily = DatabricksRunNowOperator(
        task_id="run_bronze_daily",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("bronze_daily_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_daily",
        conf={
            "batch_id": "{{ ts_nodash }}"
        }
    )

    run_bronze_daily >> trigger_silver