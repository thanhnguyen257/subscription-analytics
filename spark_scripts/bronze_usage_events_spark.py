from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from helpers import Bronze_Layer

# -----------------------------
# Create Spark session
# -----------------------------
builder = (
    SparkSession.builder
    .appName("payments_bronze")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -----------------------------
# Source config
# -----------------------------
table_name = "public.usage_events"

# Use a path that is mounted and shared between Airflow + Spark containers
bronze_path = "/spark-data/bronze/usage_events"
audit_path = "/spark-data/bronze/_audit/usage_events_validation"
expected_columns = ['id', 'data', 'created_at', 'processed']
usage_events_bronze = Bronze_Layer(spark=spark, 
                              table_name=table_name, 
                              bronze_path=bronze_path, 
                              audit_path=audit_path,
                              expected_columns=expected_columns,
                              kwargs={"id": "id"})
usage_events_bronze.save_into_bronze()

spark.stop()