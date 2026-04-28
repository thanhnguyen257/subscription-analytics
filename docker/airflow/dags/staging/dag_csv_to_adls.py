from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

@dag(
    dag_id="csv_to_adls_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["staging", "databricks"]
)
def csv_to_adls():

    run_job = DatabricksRunNowOperator(
        task_id="run_autoloader",
        databricks_conn_id="databricks",
        job_id=8266396935919,
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        },
        retries=2
    )

    run_job

csv_to_adls()