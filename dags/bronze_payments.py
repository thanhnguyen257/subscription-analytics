from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag

@dag
def create_payments_bronze():
    spark_job = SparkSubmitOperator(
        task_id="spark_task",
        application="/spark_scripts/bronze_payments_spark.py",
        conn_id="spark-conn",
        packages="io.delta:delta-spark_2.12:3.3.2,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        verbose=True,
    )

    spark_job

create_payments_bronze()
