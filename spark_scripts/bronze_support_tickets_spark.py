from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from helpers import Bronze_Layer

# -----------------------------
# Create Spark session
# -----------------------------
builder = (
    SparkSession.builder
    .appName("support_tickets_bronze")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -----------------------------
# Source config
# -----------------------------
table_name = "public.support_tickets"

# Use a path that is mounted and shared between Airflow + Spark containers
bronze_path = "/spark-data/bronze/support_tickets"
audit_path = "/spark-data/bronze/_audit/support_tickets_validation"
expected_columns = ['id', 'ticket_id', 'user_id', 'category', 'description', 'created_at']
payments_bronze = Bronze_Layer(spark=spark, 
                              table_name=table_name, 
                              bronze_path=bronze_path, 
                              audit_path=audit_path,
                              expected_columns=expected_columns,
                              kwargs={"id": "ticket_id"})
payments_bronze.save_into_bronze()

spark.stop()