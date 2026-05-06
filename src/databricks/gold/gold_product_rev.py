# Databricks notebook source
# MAGIC %md
# MAGIC # gold_product_rev

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

subs.show(2)

# COMMAND ----------

max_report_date = (
    subs
    .select( F.max(F.coalesce(F.col("end_date"), F.col("start_date"))).alias("max_date"))
    .collect()[0]["max_date"]
)

df1_temp = (
    subs
    .withColumn("start_month", F.trunc("start_date", "MM"))
    .withColumn(
        "end_month",
        F.trunc(
            F.coalesce(
                F.col("end_date"),
                F.lit(max_report_date).cast("date")
            ),
            "MM"
        )
    )
    .filter(F.col("start_month").isNotNull())
    .filter(F.col("end_month").isNotNull())
    .filter(F.col("start_month") <= F.col("end_month"))
    .withColumn(
        "month",
        F.explode(
            F.sequence(
                F.col("start_month"),
                F.col("end_month"),
                F.expr("interval 1 month")
            )
        )
    )
    .drop("start_month", "end_month")
).select("user_id","subscription_id", "month", "plan_id")\
.join(changes.select("subscription_id", "change_type"), on="subscription_id", how="left")\
.join(plans.select("plan_id", "product_id",  "billing_cycle", "tier"), on="plan_id", how="left")\
.join(users.select("user_id"), on="user_id", how="left")\
.join(licenses.select("license_id", "subscription_id"), on="subscription_id", how="left")\
.join(products.select("product_id", "category", "product_name"), on="product_id", how="left")
display(df1_temp)

# COMMAND ----------

gold_product_plan_monthly = (
    df1_temp
    .withColumnRenamed("category", "product_category")
    .groupBy(
        "month",
        "product_id",
        "product_name",
        "product_category",
        "plan_id",
        "tier",
        "billing_cycle"
    )
    .agg(
        F.countDistinct("subscription_id").alias("active_subscriptions"),

        F.countDistinct(
            F.when(F.lower(F.col("change_type")) == "new", F.col("subscription_id"))
        ).alias("new_subscriptions"),

        F.countDistinct(
            F.when(F.lower(F.col("change_type")) == "upgrade", F.col("subscription_id"))
        ).alias("upgrade_count"),

        F.countDistinct(
            F.when(F.lower(F.col("change_type")) == "downgrade", F.col("subscription_id"))
        ).alias("downgrade_count")
    )
)

display(gold_product_plan_monthly)