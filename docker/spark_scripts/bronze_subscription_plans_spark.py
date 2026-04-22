from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from helpers import Bronze_Layer

# -----------------------------
# Create Spark session
# -----------------------------
builder = (
    SparkSession.builder
    .appName("user_bronze")
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
table_name = "public.subscription_plans"

# Use a path that is mounted and shared between Airflow + Spark containers
bronze_path = "/spark-data/bronze/subscription_plans"
audit_path = "/spark-data/bronze/_audit/subscription_plans_validation"
expected_columns = ['id',
                    'plan_id',
                    'product_id',
                    'tier',
                    'billing_cycle',
                    'price',
                    'currency',
                    'created_at']
subscription_plans_bronze = Bronze_Layer(spark=spark, 
                              jdbc_url=jdbc_url, 
                              table_name=table_name, 
                              bronze_path=bronze_path, 
                              audit_path=audit_path,
                              expected_columns=expected_columns,
                              kwargs={"id": "plan_id"})
subscription_plans_bronze.save_into_bronze()

spark.stop()