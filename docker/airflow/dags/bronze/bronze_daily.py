from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta

# def has_new_data(**context):
#     result = context["ti"].xcom_pull(task_ids="run_bronze_daily")

#     if not result:
#         return False

#     try:
#         notebook_result = result.get("notebook_output", {}).get("result")

#         print(f"[DEBUG] Notebook result: {notebook_result}")

#         return notebook_result is not None and int(notebook_result) > 0

#     except Exception as e:
#         print(f"[ERROR] Parsing failed: {e}")
#         return False

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
        },
        do_xcom_push=True
    )

    # check_new_data = ShortCircuitOperator(
    #     task_id="check_new_data",
    #     python_callable=has_new_data
    # )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_daily",
        conf={
            "batch_id": "{{ ts_nodash }}"
        }
    )

    # run_bronze_daily >> check_new_data >> trigger_silver
    run_bronze_daily >> trigger_silver