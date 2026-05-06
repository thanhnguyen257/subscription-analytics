# Databricks notebook source
# MAGIC %md
# MAGIC The cohort table is about tracking how well each starting group of users/subscriptions stays active over time.
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
silver_db = "/Volumes/mock26/default/report_silver"
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

users.count(), products.count(), allocations.count(), usage.count(), plans.count(), subs.count(), changes.count(), licenses.count(), tickets.count(), payments.count()

# COMMAND ----------

display(plans)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cohort_month for overall cohort

# COMMAND ----------

display(subs.limit(10))

# COMMAND ----------

df0 = subs.join(plans.select("plan_id", "product_id"), on="plan_id", how="left").join(products.select("product_id", "product_name"), on="product_id", how="left").join(users.select("user_id", "acquisition_channel", "country"), on="user_id", how="left")
display(df0)

# COMMAND ----------

w_overall = Window.partitionBy("user_id")
w_product = Window.partitionBy("user_id", "product_name")
w_plan = Window.partitionBy("user_id", "plan_id")
w_channel = Window.partitionBy("user_id", "acquisition_channel")
w_country = Window.partitionBy("user_id", "country")

df1 = (
    df0
    .withColumn("cohort_month_overall", F.trunc(F.min("start_date").over(w_overall), "month"))
    .withColumn("product_cohort_month", F.trunc(F.min("start_date").over(w_product), "month"))
    .withColumn("plan_cohort_month", F.trunc(F.min("start_date").over(w_plan), "month"))
    .withColumn("acquisition_channel_cohort_month", F.trunc(F.min("start_date").over(w_channel), "month"))
    .withColumn("country_cohort_month", F.trunc(F.min("start_date").over(w_country), "month"))
    .select(
        "user_id",
        "cohort_month_overall",
        "product_cohort_month",
        "plan_cohort_month",
        "acquisition_channel_cohort_month",
        "country_cohort_month",
        "product_name",
        "plan_id",
        "acquisition_channel",
        "country"
    )
)


# COMMAND ----------

display(df1)

# COMMAND ----------

payment_activity = (
    payments
    .filter(F.col("payment_status") == "success")
    .join(
        df0.select("subscription_id", "user_id", "plan_id", "acquisition_channel", "country", "product_name"),
        on="subscription_id",
        how="left"
    )
    .withColumn("activity_month", F.trunc("payment_date", "month"))
    .select(
        "user_id",
        "subscription_id",
        "plan_id",
        "activity_month",
        "acquisition_channel",
        "country",
        "product_name"
    )
    .dropDuplicates()
)

overall_cohort = (
    df1
    .select("user_id", "cohort_month_overall")
    .dropDuplicates()
)
product_cohort = (
    df1
    .select("user_id", "product_name", "product_cohort_month")
    .dropDuplicates()
)

plan_cohort = (
    df1
    .select("user_id", "plan_id", "plan_cohort_month")
    .dropDuplicates()
)

channel_cohort = (
    df1
    .select("user_id", "acquisition_channel", "acquisition_channel_cohort_month")
    .dropDuplicates()
)

country_cohort = (
    df1
    .select("user_id", "country", "country_cohort_month")
    .dropDuplicates()
)

df2 = (
    payment_activity
    .join(overall_cohort, on="user_id", how="left")
    .join(plan_cohort, on=["user_id", "plan_id"], how="left")
    .join(channel_cohort, on=["user_id", "acquisition_channel"], how="left")
    .join(country_cohort, on=["user_id", "country"], how="left")
    .join(product_cohort, on=["user_id", "product_name"], how="left")
    .withColumn(
        "months_since_signup",
        (F.year("activity_month") - F.year("cohort_month_overall")) * 12
        + (F.month("activity_month") - F.month("cohort_month_overall"))
    )
    .withColumn(
        "months_since_plan_start",
        (F.year("activity_month") - F.year("plan_cohort_month")) * 12
        + (F.month("activity_month") - F.month("plan_cohort_month"))
        
    )
    .withColumn(
        "months_since_channel_start",
        (F.year("activity_month") - F.year("acquisition_channel_cohort_month")) * 12
        + (F.month("activity_month") - F.month("acquisition_channel_cohort_month"))
    )
    .withColumn(
        "months_since_country_start",
        (F.year("activity_month") - F.year("country_cohort_month")) * 12
        + (F.month("activity_month") - F.month("country_cohort_month"))
    ).withColumn(
    "months_since_product_start",
    (F.year("activity_month") - F.year("product_cohort_month")) * 12
    + (F.month("activity_month") - F.month("product_cohort_month")))
    .filter(
    (F.col("months_since_signup") >= 0) &
    (F.col("months_since_signup") <= 12))
    .filter(
    (F.col("months_since_plan_start") >= 0) &
    (F.col("months_since_plan_start") <= 12))
    .filter(
    (F.col("months_since_channel_start") >= 0) &
    (F.col("months_since_channel_start") <= 12))
    .filter(
    (F.col("months_since_country_start") >= 0) &
    (F.col("months_since_country_start") <= 12))
    .filter(
    (F.col("months_since_product_start") >= 0) &
    (F.col("months_since_product_start") <= 12)))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple cohort tables:
# MAGIC
# MAGIC 1. cohort_retention_overall
# MAGIC
# MAGIC 2. cohort_retention_by_product
# MAGIC
# MAGIC 3. cohort_retention_by_plan
# MAGIC
# MAGIC 4. cohort_retention_by_country
# MAGIC
# MAGIC 5. cohort_retention_by_acquisition_channel

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Overall

# COMMAND ----------

cohort_size_overall = (
    df2
    .select("cohort_month_overall", "user_id")
    .dropDuplicates()
    .groupBy("cohort_month_overall")
    .agg(
        F.count("user_id").alias("cohort_size")
    )
)

# retained users by month since signup
retained_users_overall = (
    df2
    .select("cohort_month_overall", "months_since_signup", "user_id")
    .dropDuplicates()
    .groupBy("cohort_month_overall", "months_since_signup")
    .agg(
        F.count("user_id").alias("retained_users")
    )
)

# retention table
cohort_retention_overall = (
    retained_users_overall
    .join(
        cohort_size_overall,
        on="cohort_month_overall",
        how="left"
    )
    .withColumn(
        "retention_rate",
        F.round(F.col("retained_users") / F.col("cohort_size"), 2)
    )
    .orderBy("cohort_month_overall", "months_since_signup")
    .withColumn("created_at", F.current_timestamp())
)

display(cohort_retention_overall)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Product

# COMMAND ----------

cohort_size_product = (
    df2
    .select("product_cohort_month", "product_name", "user_id")
    .dropDuplicates()
    .groupBy("product_cohort_month", "product_name")
    .agg(
        F.count("user_id").alias("cohort_size")
    )
)

retained_users_product = (
    df2
    .select("product_cohort_month", "product_name", "months_since_product_start", "user_id")
    .dropDuplicates()
    .groupBy("product_cohort_month", "product_name", "months_since_product_start")
    .agg(
        F.count("user_id").alias("retained_users")
    )
)

cohort_retention_product = (
    retained_users_product
    .join(
        cohort_size_product,
        on=["product_cohort_month", "product_name"],
        how="left"
    )
    .withColumn(
        "retention_rate",
        F.round(F.col("retained_users") / F.col("cohort_size"), 2)
    )
    .orderBy("product_cohort_month", "product_name", "months_since_product_start")
    .withColumn("created_at", F.current_timestamp())
)

display(cohort_retention_product)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. plan

# COMMAND ----------

cohort_size_plan = (
    df2
    .select("plan_cohort_month", "plan_id", "user_id")
    .dropDuplicates()
    .groupBy("plan_cohort_month", "plan_id")
    .agg(
        F.count("user_id").alias("cohort_size")
    )
)

retained_users_plan = (
    df2
    .select("plan_cohort_month", "plan_id", "months_since_plan_start", "user_id")
    .dropDuplicates()
    .groupBy("plan_cohort_month", "plan_id", "months_since_plan_start")
    .agg(
        F.count("user_id").alias("retained_users")
    )
)

cohort_retention_plan = (
    retained_users_plan
    .join(
        cohort_size_plan,
        on=["plan_cohort_month", "plan_id"],
        how="left"
    )
    .withColumn(
        "retention_rate",
        F.round(F.col("retained_users") / F.col("cohort_size"), 2)
    )
    .orderBy("plan_cohort_month", "plan_id", "months_since_plan_start")
    .withColumn("created_at", F.current_timestamp())
)

display(cohort_retention_plan)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. acquisition_channel

# COMMAND ----------

cohort_size_channel = (
    df2
    .select("acquisition_channel_cohort_month", "acquisition_channel", "user_id")
    .dropDuplicates()
    .groupBy("acquisition_channel_cohort_month", "acquisition_channel")
    .agg(
        F.count("user_id").alias("cohort_size")
    )
)

retained_users_channel = (
    df2
    .select(
        "acquisition_channel_cohort_month",
        "acquisition_channel",
        "months_since_channel_start",
        "user_id"
    )
    .dropDuplicates()
    .groupBy(
        "acquisition_channel_cohort_month",
        "acquisition_channel",
        "months_since_channel_start"
    )
    .agg(
        F.count("user_id").alias("retained_users")
    )
)

cohort_retention_channel = (
    retained_users_channel
    .join(
        cohort_size_channel,
        on=["acquisition_channel_cohort_month", "acquisition_channel"],
        how="left"
    )
    .withColumn(
        "retention_rate",
        F.round(F.col("retained_users") / F.col("cohort_size"), 2)
    )
    .orderBy(
        "acquisition_channel_cohort_month",
        "acquisition_channel",
        "months_since_channel_start"
    )
    .withColumn("created_at", F.current_timestamp())
)

display(cohort_retention_channel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Country

# COMMAND ----------

cohort_size_country = (
    df2
    .select("country_cohort_month", "country", "user_id")
    .dropDuplicates()
    .groupBy("country_cohort_month", "country")
    .agg(
        F.count("user_id").alias("cohort_size")
    )
)

retained_users_country = (
    df2
    .select(
        "country_cohort_month",
        "country",
        "months_since_country_start",
        "user_id"
    )
    .dropDuplicates()
    .groupBy(
        "country_cohort_month",
        "country",
        "months_since_country_start"
    )
    .agg(
        F.count("user_id").alias("retained_users")
    )
)

cohort_retention_country = (
    retained_users_country
    .join(
        cohort_size_country,
        on=["country_cohort_month", "country"],
        how="left"
    )
    .withColumn(
        "retention_rate",
        F.round(F.col("retained_users") / F.col("cohort_size"), 2)
    )
    .orderBy(
        "country_cohort_month",
        "country",
        "months_since_country_start"
    )
    .withColumn("created_at", F.current_timestamp())
)

display(cohort_retention_country)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving
# MAGIC
# MAGIC If same cohort_month + months_since_signup exists:
# MAGIC     replace/update the row
# MAGIC
# MAGIC If it does not exist:
# MAGIC     insert new row

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

# tables = [
#     "mock26.default.gold_cohort_retention_overall",
#     "mock26.default.gold_cohort_retention_by_product",
#     "mock26.default.gold_cohort_retention_by_plan",
#     "mock26.default.gold_cohort_retention_by_country",
#     "mock26.default.gold_cohort_retention_by_acquisition_channel"
# ]

# for table in tables:
#     spark.sql(f"DROP TABLE IF EXISTS {table}")
# for table in tables:
#     print(table, spark.catalog.tableExists(table))


# COMMAND ----------

overall_keys = [
    "cohort_month",
    "months_since_signup"
]
upsert_delta_table(
    cohort_retention_overall,
    "mock26.default.gold_cohort_retention_overall",
    overall_keys
)
product_keys = [
    "cohort_month",
    "months_since_signup",
    "product_id"
]

upsert_delta_table(
    cohort_retention_product,
    "mock26.default.gold_cohort_retention_by_product",
    product_keys
)
plan_keys = [
    "cohort_month",
    "months_since_signup",
    "plan_id"
]

upsert_delta_table(
    cohort_retention_plan,
    "mock26.default.gold_cohort_retention_by_plan",
    plan_keys
)
country_keys = [
    "cohort_month",
    "months_since_signup",
    "country"
]

upsert_delta_table(
    cohort_retention_country,
    "mock26.default.gold_cohort_retention_by_country",
    country_keys
)
channel_keys = [
    "cohort_month",
    "months_since_signup",
    "acquisition_channel"
]

upsert_delta_table(
    cohort_retention_channel,
    "mock26.default.gold_cohort_retention_by_acquisition_channel",
    channel_keys
)