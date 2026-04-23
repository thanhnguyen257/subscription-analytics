from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.sdk import dag

@dag(
        dag_id="create_license_allocations_bronze",
        schedule="@monthly",
        catchup=False,
        start_date=datetime(2026, 4, 1),
        tags=["bronze"]
)
def create_products_bronze():
    spark_job = SparkSubmitOperator(
        task_id="spark_task",
        application="/spark_scripts/bronze_license_allocations_spark.py",
        conn_id="spark-conn",
        packages="io.delta:delta-spark_2.12:3.3.2,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        verbose=True,
    )

    spark_job

create_products_bronze()
