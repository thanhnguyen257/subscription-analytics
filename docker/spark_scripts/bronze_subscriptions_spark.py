from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip, DeltaTable
from helpers import Bronze_Layer

# -----------------------------
# Create Spark session
# -----------------------------
builder = (
    SparkSession.builder
    .appName("subscription_bronze")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -----------------------------
# Source config
# -----------------------------
jdbc_url = "jdbc:postgresql://postgres:5432/staging_area"
table_name = "public.subscriptions"

# Use a path that is mounted and shared between Airflow + Spark containers
bronze_path = "/spark-data/bronze/subscriptions"
audit_path = "/spark-data/bronze/_audit/subscriptions_validation"
expected_columns = ['id',
                    'subscription_id',
                    'user_id',
                    'plan_id',
                    'start_date',
                    'end_date',
                    'status',
                    'current_mrr',
                    'created_at']
subscription_bronze = Bronze_Layer(spark=spark, 
                              jdbc_url=jdbc_url, 
                              table_name=table_name, 
                              bronze_path=bronze_path, 
                              audit_path=audit_path,
                              expected_columns=expected_columns,
                              kwargs={"id": "subscription_id"})
subscription_bronze.save_into_bronze()

spark.stop()