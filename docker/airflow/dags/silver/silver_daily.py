from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="silver_daily",
    start_date=datetime(2026, 4, 1),
    schedule=None,  # IMPORTANT: only triggered, not scheduled
    catchup=False,
    tags=["silver"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver_transform",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("silver_job_id")),
        notebook_params={
            "batch_id": "{{ dag_run.conf.get('batch_id') }}"
        }
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="gold_daily",
        conf={
            "batch_id": "{{ dag_run.conf.get('batch_id') }}"
        }
    )

    run_silver >> trigger_gold