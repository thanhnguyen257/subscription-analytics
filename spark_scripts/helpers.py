from pyspark.sql import functions as F
from delta import DeltaTable
class Bronze_Layer:
    def __init__(self, spark, table_name, bronze_path, audit_path, expected_columns, kwargs,db="subscription"):
        self.spark = spark
        self.table_name = table_name
        self.bronze_path = bronze_path
        self.audit_path = audit_path
        self.expected_columns = expected_columns
        self.kwargs = kwargs
        self.jdbc_url = f"jdbc:postgresql://postgres_db:5432/{db}"


    def get_raw_table_from_db(self):
        df_raw = (
        self.spark.read
        .format("jdbc")
        .option("url", self.jdbc_url)
        .option("dbtable", self.table_name)
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .load())
        return df_raw

    def get_clean(self):
        #expected_columns = ['id', 'product_id', 'product_name', 'category', 'created_at']
        df_raw = self.get_raw_table_from_db()
        actual_columns = df_raw.columns

        missing_cols = sorted(list(set(self.expected_columns) - set(actual_columns)))
        extra_cols = sorted(list(set(actual_columns) - set(self.expected_columns)))

        print(f"Schema Audit for {self.table_name}:")
        print(f" - Missing columns: {missing_cols if missing_cols else 'None'}")
        print(f" - Extra columns: {extra_cols if extra_cols else 'None'}")

        # Optional: select only expected columns that actually exist
        existing_expected_cols = [c for c in self.expected_columns if c in actual_columns]
        df_clean = df_raw.select(*existing_expected_cols)
        return df_clean
    def save_into_bronze(self):
        df_raw = self.get_raw_table_from_db()
        df_clean = self.get_clean()
        # -----------------------------
        # Add metadata columns
        # -----------------------------
        df_bronze = (
            df_clean
            .withColumn("ingest_time", F.current_timestamp())
            .withColumn("source_identifier", F.lit(f"{self.jdbc_url}/{self.table_name}"))
            .withColumn("batch_id", F.lit(self.spark.sparkContext.applicationId))
        )

        # -----------------------------
        # Validation stats
        # -----------------------------
        current_id = self.kwargs.get("id")
        auto_id = "id"
        if "id" not in df_bronze.columns:
            auto_id = current_id

        validation_stats = df_bronze.select(
            F.lit(self.table_name).alias("dataset"),
            F.count("*").alias("row_count"),
            F.sum(F.when(F.col(current_id).isNull(), 1).otherwise(0)).alias("user_id_null_count"),
            F.countDistinct(auto_id).alias("id_distinct_count"),
            F.lit(df_raw.schema.json()).alias("captured_schema_json"),
            F.current_timestamp().alias("audit_time"),
            F.lit(self.spark.sparkContext.applicationId).alias("batch_id"))

        print("Bronze Data Sample:")
        df_bronze.show(5, truncate=False)
        print("Validation Statistics:")
        validation_stats.show(truncate=False)

        # -----------------------------
        # Save BRONZE as Delta
        # -----------------------------
        (
            df_bronze.write
            .format("delta")
            .mode("overwrite")              # change to append if needed
            .option("overwriteSchema", "true")
            .save(self.bronze_path)
        )

        # -----------------------------
        # Save audit stats as Delta
        # -----------------------------
        (
            validation_stats.write
            .format("delta")
            .mode("append")
            .save(self.audit_path)
        )

        # -----------------------------
        # Read back to verify
        # -----------------------------
        print("Verify bronze Delta data:")
        self.spark.read.format("delta").load(self.bronze_path).show(5, truncate=False)

        print("Is Delta table?")
        print(DeltaTable.isDeltaTable(self.spark, self.bronze_path))