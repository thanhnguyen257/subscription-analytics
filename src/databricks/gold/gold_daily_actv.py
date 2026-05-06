# Databricks notebook source
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
import numpy as np
print(pyspark.__version__)


# COMMAND ----------

builder = (
    SparkSession.builder
    .appName("user_bronze")
)
spark = builder.getOrCreate()
silver_db = "/Volumes/mock26/default/report_silver/report_silver/"
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
# MAGIC ## Features:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | date | Activity date |
# MAGIC | dau | Daily active users |
# MAGIC | active_subscriptions | Active subscriptions on that day |
# MAGIC | login_events | Number of login events |
# MAGIC | feature_events | Number of feature usage events |
# MAGIC | content_view_events | Number of content/content-view events |
# MAGIC | total_events | Total activity events |
# MAGIC | avg_events_per_user | total_events / dau |
# MAGIC | inactive_users | Users/subscriptions with no recent activity, if available |
# MAGIC | product_id | Product used |
# MAGIC | plan_id | Plan used |
# MAGIC

# COMMAND ----------

display(subs)

# COMMAND ----------

df1 = (usage.join(
    subs.select("plan_id", "subscription_id", "start_date", "end_date"),
    on="subscription_id",
    how="left"
).join(plans.select("plan_id", "product_id"), on="plan_id"))
display(df1.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## create timestamp

# COMMAND ----------

today_date = "2027-04-11"

df1_with_event_timestamp = (
    df1
    .drop("event_timestamp")
    .withColumn("_start_date", F.to_date("start_date"))
    .withColumn("_today", F.to_date(F.lit(today_date)))
    .withColumn(
        "_days_range",
        F.datediff(F.col("_today"), F.col("_start_date"))
    )
    .withColumn(
        "_random_days",
        F.floor(F.rand(42) * (F.col("_days_range") + 1)).cast("int")
    )
    .withColumn(
        "_event_date",
        F.date_add(F.col("_start_date"), F.col("_random_days"))
    )
    .withColumn(
        "event_timestamp",
        F.expr("""
            timestampadd(
                SECOND,
                cast(floor(rand(43) * 86400) as int),
                cast(_event_date as timestamp)
            )
        """)
    )
    .drop("_start_date", "_today", "_days_range", "_random_days", "_event_date")
)

display(df1_with_event_timestamp)


# COMMAND ----------

df1_with_event_timestamp.select(
    F.min(F.to_date("event_timestamp")).alias("min"),
    F.max(F.to_date("event_timestamp")).alias("max")
).show()


# COMMAND ----------

df2 = df1_with_event_timestamp

# COMMAND ----------

gold_daily_actv_df = (
    df2
    .withColumn("date", F.to_date("event_timestamp"))
    .withColumn(
        "event_type_lower",
        F.lower(F.coalesce(F.col("event_type"), F.lit("")))
    )
    .groupBy("date")
    .agg(
        F.countDistinct("user_id").alias("dau"),

        F.countDistinct("subscription_id").alias("active_subscriptions"),

        F.countDistinct("product_id").alias("products_used"),

        F.countDistinct("plan_id").alias("plans_used"),

        F.sum(
            F.when(F.col("event_type_lower").contains("login"), 1).otherwise(0)
        ).cast("long").alias("login_events"),

        F.sum(
            F.when(F.col("feature_name").isNotNull(), 1).otherwise(0)
        ).cast("long").alias("feature_events"),

        F.sum(
            F.when(
                F.col("content_id").isNotNull()
                | F.col("event_type_lower").contains("content")
                | F.col("event_type_lower").contains("view"),
                1
            ).otherwise(0)
        ).cast("long").alias("content_view_events"),

        F.count("*").cast("long").alias("total_events"),

        # F.max("created_at").alias("created_at")
    )
    .withColumn(
        "avg_events_per_user",
        F.when(
            F.col("dau") > 0,
            F.round(F.col("total_events") / F.col("dau"),2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn("saved_at", F.current_timestamp())
    .select(
        "date",
        "dau",
        "active_subscriptions",
        "login_events",
        "feature_events",
        "content_view_events",
        "total_events",
        "avg_events_per_user",
        "products_used",
        "plans_used"
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
    df=gold_daily_actv_df.withColumn("created_at", F.current_timestamp()),
    table_name="mock26.default.gold_daily_actv_df",
    merge_keys=["date"]
)

# COMMAND ----------

upsert_delta_table(
    df=gold_daily_actv_df.withColumn("created_at", F.current_timestamp()),
    table_name="mock26.default.gold_daily_actv_df",
    merge_keys=["date"]
)