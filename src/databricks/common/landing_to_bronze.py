from pyspark.sql.functions import col
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class LandingToBronzeIngest:

    def __init__(
        self,
        spark,
        entity,
        landing_base_path="/Volumes/datalake_catalog/datalake_schema/landing",
        bronze_base_path="/Volumes/datalake_catalog/datalake_schema/bronze",
        watermark_col="dw_ingested_at"
    ):
        self.spark = spark
        self.entity = entity
        self.watermark_col = watermark_col

        self.landing_path = f"{landing_base_path}/{entity}"
        self.bronze_path = f"{bronze_base_path}/{entity}"

    def is_empty(self, df):
        return df.limit(1).count() == 0

    # -----------------------------
    # Get last watermark
    # -----------------------------
    def get_last_watermark(self):
        print(f"[INFO] Checking watermark for {self.entity}")

        if not DeltaTable.isDeltaTable(self.spark, self.bronze_path):
            print("[INFO] First run (no Bronze table)")
            return None

        df = self.spark.read.format("delta").load(self.bronze_path)
        wm = df.agg(F.max(self.watermark_col)).collect()[0][0]

        print(f"[INFO] Last watermark: {wm}")
        return wm

    # -----------------------------
    # Read from landing
    # -----------------------------
    def read_landing(self):
        print(f"[INFO] Reading landing data: {self.landing_path}")

        df = self.spark.read.format("delta").load(self.landing_path)

        print("[INFO] Schema:")
        df.printSchema()

        return df

    # -----------------------------
    # Apply incremental filter
    # -----------------------------
    def apply_incremental(self, df):
        last_wm = self.get_last_watermark()

        if last_wm:
            print(f"[INFO] Applying watermark filter > {last_wm}")
            df = df.filter(col(self.watermark_col) > last_wm)

        return df

    # -----------------------------
    # Write Bronze
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
        print(f"\n[STAGE] ===== LANDING → BRONZE: {self.entity} =====")

        try:
            df = self.read_landing()
            df = self.apply_incremental(df)

            if self.is_empty(df):
                print("[INFO] No new data to process")
                return

            count = df.count()
            print(f"[INFO] Rows to write: {count}")

            self.write_bronze(df)

            print("[SUCCESS] Ingestion completed")

        except Exception as e:
            print(f"[ERROR] Failed {self.entity}: {str(e)}")
            raise