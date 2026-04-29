from airflow.sdk import dag
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

@dag(
    dag_id="staging_csv_to_adls_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["staging", "databricks"]
)
def csv_to_adls():

    run_job = DatabricksRunNowOperator(
        task_id="run_autoloader",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("staging_pipline_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )

    run_job

csv_to_adls()