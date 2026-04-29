from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id="bronze_daily",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    run = DatabricksRunNowOperator(
        task_id="run_bronze_daily",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("staging_pipline_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )