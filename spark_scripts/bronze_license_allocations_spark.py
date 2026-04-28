from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from helpers import Bronze_Layer

# -----------------------------
# Create Spark session
# -----------------------------
builder = (
    SparkSession.builder
    .appName("license_allocations_bronze")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -----------------------------
# Source config
# -----------------------------
table_name = "public.license_allocations"

# Use a path that is mounted and shared between Airflow + Spark containers
bronze_path = "/spark-data/bronze/license_allocations"
audit_path = "/spark-data/bronze/_audit/license_allocations_validation"
expected_columns = ['allocation_id',
                    'license_id',
                    'seat_number',
                    'status',
                    'allocation_date',
                    'ingestion_time',
                    'batch_id']
license_allocations_bronze = Bronze_Layer(spark=spark, 
                              table_name=table_name, 
                              bronze_path=bronze_path, 
                              audit_path=audit_path,
                              expected_columns=expected_columns,
                              kwargs={"id": "allocation_id"})
license_allocations_bronze.save_into_bronze()

spark.stop()