from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip, DeltaTable
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
table_name = "public.products"

# Use a path that is mounted and shared between Airflow + Spark containers
bronze_path = "/spark-data/bronze/products"
audit_path = "/spark-data/bronze/_audit/products_validation"
expected_columns = ['id', 'product_id', 'product_name', 'category', 'created_at']
product_bronze = Bronze_Layer(spark=spark, 
                              table_name=table_name, 
                              bronze_path=bronze_path, 
                              audit_path=audit_path,
                              expected_columns=expected_columns,
                              kwargs={"id": "product_id"})
product_bronze.save_into_bronze()

spark.stop()