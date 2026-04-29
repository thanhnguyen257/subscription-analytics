with DAG(
    dag_id="bronze_usage_events_hourly",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False
) as dag:

    run = DatabricksRunNowOperator(
        task_id="run_usage_events",
        databricks_conn_id="databricks",
        job_id=int(Variable.get("staging_pipline_job_id")),
        notebook_params={
            "batch_id": "{{ ts_nodash }}"
        }
    )