# Databricks notebook source
# MAGIC %md
# MAGIC # Gold License Utilization
# MAGIC month
# MAGIC
# MAGIC total_enterprise_customers = number of enterprise customers in that month
# MAGIC
# MAGIC total_max_seats = total purchased seats
# MAGIC
# MAGIC total_allocated_seats = total assigned seats
# MAGIC
# MAGIC total_active_seats = total seats actually used
# MAGIC
# MAGIC total_unused_seats = total_max_seats - total_allocated_seats
# MAGIC
# MAGIC avg_utilization_rate = total_allocated_seats / total_max_seats
# MAGIC
# MAGIC low_utilization_customer_count = number of customers with utilization_rate < 0.50
# MAGIC
# MAGIC medium_utilization_customer_count = number of customers with utilization_rate >= 0.50 and < 0.80
# MAGIC
# MAGIC high_utilization_customer_count = number of customers with utilization_rate >= 0.80
# MAGIC
# MAGIC near_limit_customer_count = number of customers with utilization_rate >= 0.90
# MAGIC
# MAGIC enterprise_mrr = total MRR from enterprise customers
# MAGIC

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, to_timestamp, lit, create_map, upper, trim
from pyspark.sql.types import DecimalType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from functools import reduce
from itertools import chain
import sklearn
from pyspark.sql.types import DoubleType, FloatType, DecimalType
print(pyspark.__version__)


# COMMAND ----------

builder = (
    SparkSession.builder
    .appName("user_bronze")
)
spark = builder.getOrCreate()
silver_db = "/Volumes/mock26/default/report_silver/"
gold_db = "gold"
output_table = f"{gold_db}.churn_features"
snapshot_date = F.current_date()
cutoff_date = F.lit("2026-04-30").cast("date")
users = (
    spark.read
    .format("delta")
    .load(silver_db+"/users")
)
products = (
    spark.read
    .format("delta")
    .load(silver_db+"/products")
)
plans = (
    spark.read
    .format("delta")
    .load(silver_db+"/plans")
)
subs = (
    spark.read
    .format("delta")
    .load(silver_db+"/subscriptions")
)

changes = (
    spark.read
    .format("delta")
    .load(silver_db+"/subscription_changes")
)
payments = (
    spark.read
    .format("delta")
    .load(silver_db+"/payments")
)
licenses = (
    spark.read
    .format("delta")
    .load(silver_db+"/licenses")
)
allocations = (
    spark.read
    .format("delta")
    .load(silver_db+"/license_allocations")
)
usage = (
    spark.read
    .format("delta")
    .load(silver_db+"/usage_events")
)
tickets = (
    spark.read
    .format("delta")
    .load(silver_db+"/support_tickets")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## total_enterprise_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 

# COMMAND ----------

max_end_date = subs.agg(F.max("end_date").alias("max_end_date")).first()["max_end_date"]

df1_temp = (
    subs
    .withColumn("start_month", F.trunc("start_date", "MM"))
    .withColumn(
        "end_month",
        F.trunc(
            F.coalesce(
                F.col("end_date"),
                F.lit(max_end_date).cast("date")
            ),
            "MM"
        )
    )
    .filter(F.col("start_month").isNotNull())
    .filter(F.col("end_month").isNotNull())
    .filter(F.col("start_month") <= F.col("end_month"))
    .withColumn(
        "active_month",
        F.explode(
            F.sequence(
                F.col("start_month"),
                F.col("end_month"),
                F.expr("interval 1 month")
            )
        )
    )
    .drop("start_month", "end_month")
).select("user_id","subscription_id", "active_month", "plan_id")\
.join(users.select("user_id", "is_enterprise"), on="user_id")\
.filter(F.col("is_enterprise") == True)
df1 = df1_temp.groupBy("active_month")\
.agg(F.countDistinct("user_id").alias("total_enterprise_customers")).orderBy("active_month")

# COMMAND ----------

df2 = df1_temp.join(licenses.select("subscription_id", "license_id", "max_seats"), on="subscription_id", how="left").\
    join(allocations.select("license_id", "seat_number", "status"), on="license_id", how="left").drop("license_id").\
        fillna(0).\
        withColumn("utilization_rate", 
                   F.when(F.col("max_seats") > 0,
                       F.round(F.col("seat_number")/F.col("max_seats"), 2))\
                       .otherwise(0)
                   )
df2.show(10)

# COMMAND ----------

gold_license_ut = (
    df2
    .filter(F.col("is_enterprise") == True)
    .groupBy("active_month")
    .agg(
        F.countDistinct("subscription_id").alias("total_enterprise_customers"),
        F.sum("max_seats").alias("total_max_seats"),
        F.sum("seat_number").alias("total_allocated_seats"),
        F.sum(F.when(F.col("status") == "active", F.col("seat_number")).otherwise(0)).alias("total_active_seats"),
        (F.sum("max_seats") - F.sum("seat_number")).alias("total_unused_seats"),
        F.round(F.avg("utilization_rate"),2).alias("avg_utilization_rate"),
        F.countDistinct(F.when(F.col("utilization_rate") < 0.5, F.col("subscription_id"))).alias("low_utilization_customer_count"),
        F.countDistinct(F.when((F.col("utilization_rate") >= 0.5) & (F.col("utilization_rate") < 0.8), F.col("subscription_id"))).alias("medium_utilization_customer_count"),
        F.countDistinct(F.when(F.col("utilization_rate") >= 0.8, F.col("subscription_id"))).alias("high_utilization_customer_count"),
        F.countDistinct(F.when(F.col("utilization_rate") >= 0.9, F.col("subscription_id"))).alias("near_limit_customer_count")
    )
)

# COMMAND ----------

def quote_table_name(table_name):
    return ".".join([f"`{part}`" for part in table_name.split(".")])


def get_table_latest_created_at(table_name):
    quoted_table = quote_table_name(table_name)

    details = spark.sql(f"DESCRIBE DETAIL {quoted_table}").collect()[0]
    properties = details["properties"] or {}

    return properties.get("latest_created_at")


def set_table_latest_created_at(table_name, latest_created_at):
    quoted_table = quote_table_name(table_name)

    spark.sql(f"""
        ALTER TABLE {quoted_table}
        SET TBLPROPERTIES (
            'latest_created_at' = '{latest_created_at}'
        )
    """)


def upsert_delta_table(df, table_name, merge_keys):
    """
    Table-level versioned upsert.

    Rules:
    - If table does not exist: save whole dataframe.
    - If table exists:
        - get max(created_at) from current dataframe
        - compare with saved table metadata latest_created_at
        - if current max is not newer: skip
        - if current max is newer: upsert whole dataframe
    """

    if "created_at" not in df.columns:
        raise ValueError("created_at column is required")

    current_latest_created_at = (
        df
        .select(F.max(F.col("created_at")).alias("latest_created_at"))
        .collect()[0]["latest_created_at"]
    )

    if current_latest_created_at is None:
        print(f"Skipped table: {table_name}")
        print("Reason: dataframe has no created_at value")
        print("Inserted rows: 0")
        print("Updated rows: 0")
        return

    current_latest_created_at_str = str(current_latest_created_at)

    if not spark.catalog.tableExists(table_name):
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)

        set_table_latest_created_at(table_name, current_latest_created_at_str)

        print(f"Created new table: {table_name}")
        print(f"Inserted rows: {df.count()}")
        print("Updated rows: 0")
        print(f"latest_created_at: {current_latest_created_at_str}")
        return

    saved_latest_created_at = get_table_latest_created_at(table_name)

    if saved_latest_created_at is not None:
        should_skip = spark.sql(f"""
            SELECT
                to_timestamp('{current_latest_created_at_str}')
                <= to_timestamp('{saved_latest_created_at}')
                AS should_skip
        """).collect()[0]["should_skip"]

        if should_skip:
            print(f"Skipped table: {table_name}")
            print(f"Reason: current dataframe is not newer than saved table")
            print(f"Current latest created_at: {current_latest_created_at_str}")
            print(f"Saved latest created_at: {saved_latest_created_at}")
            print("Inserted rows: 0")
            print("Updated rows: 0")
            return

    target = DeltaTable.forName(spark, table_name)

    merge_condition = " AND ".join([
        f"target.`{key}` <=> source.`{key}`"
        for key in merge_keys
    ])

    update_set = {
        col: f"source.`{col}`"
        for col in df.columns
    }

    insert_set = {
        col: f"source.`{col}`"
        for col in df.columns
    }

    target.alias("target") \
        .merge(
            df.alias("source"),
            merge_condition
        ) \
        .whenMatchedUpdate(
            condition="to_timestamp(source.`created_at`) > to_timestamp(target.`created_at`)",
            set=update_set
        ) \
        .whenNotMatchedInsert(
            values=insert_set
        ) \
        .execute()

    set_table_latest_created_at(table_name, current_latest_created_at_str)

    history = spark.sql(f"DESCRIBE HISTORY {quote_table_name(table_name)} LIMIT 1")
    metrics = history.select("operationMetrics").collect()[0]["operationMetrics"]

    inserted_rows = int(metrics.get("numTargetRowsInserted", 0))
    updated_rows = int(metrics.get("numTargetRowsUpdated", 0))

    print(f"Upserted table: {table_name}")
    print(f"Inserted rows: {inserted_rows}")
    print(f"Updated rows: {updated_rows}")
    print(f"latest_created_at: {current_latest_created_at_str}")


# COMMAND ----------

upsert_delta_table(
    df=gold_license_ut.withColumn("created_at", F.current_timestamp()),
    table_name="mock26.default.gold_license_ut",
    merge_keys=["active_month"]
)

# COMMAND ----------

upsert_delta_table(
    df=gold_license_ut.withColumn("created_at", F.current_timestamp()),
    table_name="mock26.default.gold_license_ut",
    merge_keys=["active_month"]
)