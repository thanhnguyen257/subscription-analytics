from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

with DAG(
    dag_id="silver_daily",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["silver"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_daily",
        external_dag_id="bronze_daily",
        external_task_id="run_bronze_daily",
        timeout=3600,
        poke_interval=60
    )

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver_transform",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("silver_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )

    wait_for_bronze >> run_silver