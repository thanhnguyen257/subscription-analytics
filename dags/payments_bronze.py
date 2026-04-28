from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag

# Định nghĩa DAG với decorator, không truyền tham số vào DAG() nữa
@dag
def create_payments_bronze():
    # Sử dụng SparkSubmitOperator để gửi job tới Spark Cluster
    spark_job = SparkSubmitOperator(
    task_id="spark_task",
    application="/opt/airflow/spark_scripts/bronze_payments_spark.py",
    conn_id="spark-conn",
    packages="org.postgresql:postgresql:42.7.3",
    )

    spark_job  # Kết nối các task trong DAG

create_payments_bronze()