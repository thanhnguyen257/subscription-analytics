from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid


class PostgresBronzeIngest:

    def __init__(
        self,
        spark,
        table_name,
        bronze_path,
        dbutils,
        watermark_col="dw_ingested_at",
        secret_scope="postgres-creds",
        batch_id=None
    ):
        self.spark = spark
        self.table_name = table_name
        self.bronze_path = bronze_path
        self.watermark_col = watermark_col
        self.dbutils = dbutils
        self.batch_id = batch_id or str(uuid.uuid4())

        # -----------------------------
        # Load credentials from Databricks Secrets
        # -----------------------------
        self.host = self.dbutils.secrets.get(secret_scope, "postgres-host")
        self.port = self.dbutils.secrets.get(secret_scope, "postgres-port")
        self.db = self.dbutils.secrets.get(secret_scope, "postgres-db")
        self.user = self.dbutils.secrets.get(secret_scope, "postgres-user")
        self.password = self.dbutils.secrets.get(secret_scope, "postgres-password")

        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    # -----------------------------
    # Get last watermark
    # -----------------------------
    def get_last_watermark(self):
        print(f"[INFO] Checking watermark for {self.table_name}")

        if not DeltaTable.isDeltaTable(self.spark, self.bronze_path):
            print("[INFO] No existing Delta table (first run)")
            return None

        df = self.spark.read.format("delta").load(self.bronze_path)

        wm = df.agg(F.max(self.watermark_col)).collect()[0][0]

        print(f"[INFO] Last watermark: {wm}")
        return wm

    # -----------------------------
    # Build incremental query
    # -----------------------------
    def build_query(self, last_wm):
        if last_wm:
            # Safe timestamp format
            last_wm_str = last_wm.isoformat(sep=" ")

            query = f"""
            (SELECT *
             FROM {self.table_name}
             WHERE {self.watermark_col} > TIMESTAMP '{last_wm_str}') t
            """
        else:
            query = f"(SELECT * FROM {self.table_name}) t"

        return query

    # -----------------------------
    # Read incremental
    # -----------------------------
    def read_incremental(self):
        last_wm = self.get_last_watermark()
        query = self.build_query(last_wm)

        print(f"[INFO] Executing JDBC query:\n{query}")

        df = (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", query)
            .option("user", self.user)
            .option("password", self.password)

            # 🔥 REQUIRED for Azure PostgreSQL
            .option("ssl", "true")
            .option("sslmode", "require")

            .option("driver", "org.postgresql.Driver")
            .load()
        )

        return df

    # -----------------------------
    # Enrich metadata
    # -----------------------------
    def enrich(self, df):
        print("[INFO] Adding metadata columns")

        return (
            df.withColumn("ingest_time", F.current_timestamp())
            .withColumn("batch_id", F.lit(self.batch_id))
            .withColumn("source_table", F.lit(self.table_name))
        )

    # -----------------------------
    # Write Bronze (append-only)
    # -----------------------------
    def write_bronze(self, df):
        print(f"[INFO] Writing to Bronze: {self.bronze_path}")

        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.bronze_path)
        )

    # -----------------------------
    # Run pipeline
    # -----------------------------
    def run(self):
        print(f"\n[STAGE] ===== POSTGRES INGEST: {self.table_name} =====")

        try:
            df = self.read_incremental()

            # Better empty check
            if df.limit(1).count() == 0:
                print("[INFO] No new data found")
                return

            df = self.enrich(df)

            count = df.count()
            print(f"[INFO] Rows to write: {count}")

            self.write_bronze(df)

            print("[SUCCESS] Ingestion completed")

        except Exception as e:
            print(f"[ERROR] Failed ingestion: {str(e)}")
            raise