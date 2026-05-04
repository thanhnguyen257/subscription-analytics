from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

with DAG(
    dag_id="gold_daily",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["gold"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_daily",
        external_task_id="run_silver_transform",
        timeout=3600,
        poke_interval=60
    )

    run_gold = DatabricksRunNowOperator(
        task_id="run_gold_aggregation",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("gold_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )

    wait_for_silver >> run_gold