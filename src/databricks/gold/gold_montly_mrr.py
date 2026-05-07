# Databricks notebook source
# MAGIC %md
# MAGIC # Gold_monthly_mrr

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

# COMMAND ----------

silver_db = "/Volumes/mock26/default/report_silver/report_silver/"
gold_db = "gold"
output_table = f"{gold_db}.churn_features"
snapshot_date = F.current_date()
cutoff_date = F.lit("2026-04-30").cast("date")

# COMMAND ----------

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
# MAGIC ## Company-level MRR

# COMMAND ----------

# =========================================================
# Join subscriptions with plans
# =========================================================
subs_plan = (
    subs
    .select(
        "user_id",
        "subscription_id",
        "plan_id",
        "start_date",
        "end_date",
        "status",
        "created_at"
    )
    .join(
        plans.select("plan_id", "billing_cycle", "price", "currency"),
        on="plan_id",
        how="left"
    )
    .withColumn("billing_cycle", F.lower(F.trim(F.col("billing_cycle"))))
)

# =========================================================
# Keep only successful payments
#    Use ONE consistent amount field for MRR reporting
#    Here I use `amount` because it appears to be normalized/base currency
# =========================================================
payments_success = (
    payments
    .filter(F.lower(F.trim(F.col("payment_status"))) == "success")
    .filter(F.col("payment_date").isNotNull())
    .dropDuplicates(["payment_id"])
    .select(
        "subscription_id",
        "payment_id",
        "payment_date",
        "amount"
    )
)

# =========================================================
# Join subs + plans + successful payments
# =========================================================
subs_plan_pay = (
    subs_plan
    .join(payments_success, on="subscription_id", how="left")
)

# =========================================================
# Derive payment coverage and monthly-equivalent MRR
# =========================================================
paid_coverage = (
    subs_plan_pay
    .filter(F.col("payment_id").isNotNull())
    .filter(F.col("billing_cycle").isin("monthly", "annual"))
    .withColumn(
        "months_covered",
        F.when(F.col("billing_cycle") == "monthly", F.lit(1))
         .when(F.col("billing_cycle") == "annual", F.lit(12))
    )
    .withColumn(
        "monthly_equiv_mrr",
        F.when(F.col("billing_cycle") == "monthly", F.col("amount"))
         .when(F.col("billing_cycle") == "annual", F.col("amount") / F.lit(12.0))
    )
    .withColumn("payment_month", F.trunc(F.col("payment_date"), "month"))
    .withColumn("coverage_end_month", F.add_months(F.col("payment_month"), F.col("months_covered") - 1))
)

# =========================================================
# Expand each payment into all months it covers
# =========================================================
expanded_paid_months = (
    paid_coverage
    .withColumn(
        "month",
        F.explode(
            F.expr("sequence(payment_month, coverage_end_month, interval 1 month)")
        )
    )
    .withColumn("month_end", F.last_day(F.col("month")))
)

# =========================================================
# Keep only subscription-months where subscription is active at month end
#    Use >= if end_date means the last active day is included
# =========================================================
active_paid_months = (
    expanded_paid_months
    .filter(F.col("start_date").isNotNull())
    .filter(F.col("start_date") <= F.col("month_end"))
    .filter(
        F.col("end_date").isNull() |
        (F.col("end_date") >= F.col("month_end"))
    )
)

# =========================================================
# Deduplicate at subscription_id + month
#    Use MAX to avoid double-counting overlapping duplicate payments
# =========================================================
subscription_month_mrr = (
    active_paid_months
    .groupBy("user_id", "subscription_id", "month")
    .agg(
        F.max("monthly_equiv_mrr").alias("subscription_month_mrr")
    )
)

# =========================================================
# Aggregate to user-level current MRR per month
# =========================================================
user_month_mrr = (
    subscription_month_mrr
    .groupBy("user_id", "month")
    .agg(
        F.sum("subscription_month_mrr").alias("current_mrr")
    )
)

# =========================================================
# Build a full user-month spine so missing months become 0 MRR
#    This is important for correct expansion / churn logic
# =========================================================
min_max_month = user_month_mrr.agg(
    F.min("month").alias("min_month"),
    F.max("month").alias("max_month")
).collect()[0]

min_month = min_max_month["min_month"]
max_month = min_max_month["max_month"]

calendar_months = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{min_month}'),
                to_date('{max_month}'),
                interval 1 month
            )
        ) AS month
        """
    )
)

all_users = subs.select("user_id").distinct()

user_month_spine = all_users.crossJoin(calendar_months)

user_month_mrr_full = (
    user_month_spine
    .join(user_month_mrr, on=["user_id", "month"], how="left")
    .fillna({"current_mrr": 0.0})
)

# =========================================================
# Previous month MRR per user
# =========================================================
w = Window.partitionBy("user_id").orderBy("month")

user_mrr_movement = (
    user_month_mrr_full
    .withColumn("prev_mrr", F.lag("current_mrr").over(w))
    .withColumn("prev_mrr", F.coalesce(F.col("prev_mrr"), F.lit(0.0)))
)

# =========================================================
# Derive MRR movements
# =========================================================
user_mrr_movement = (
    user_mrr_movement
    .withColumn(
        "expansion_mrr",
        F.when(
            (F.col("prev_mrr") > 0) &
            (F.col("current_mrr") > F.col("prev_mrr")),
            F.col("current_mrr") - F.col("prev_mrr")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "new_mrr",
        F.when(
            (F.col("prev_mrr") == 0) &
            (F.col("current_mrr") > 0),
            F.col("current_mrr")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "contraction_mrr",
        F.when(
            (F.col("prev_mrr") > 0) &
            (F.col("current_mrr") > 0) &
            (F.col("current_mrr") < F.col("prev_mrr")),
            F.col("prev_mrr") - F.col("current_mrr")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "churned_mrr",
        F.when(
            (F.col("prev_mrr") > 0) &
            (F.col("current_mrr") == 0),
            F.col("prev_mrr")
        ).otherwise(F.lit(0.0))
    )
)

# =========================================================
# Company-level monthly totals
# =========================================================
df1 = (
    user_mrr_movement
    .groupBy("month")
    .agg(
        F.sum("current_mrr").alias("current_mrr"),
        F.sum("new_mrr").alias("new_mrr"),
        F.sum("expansion_mrr").alias("expansion_mrr"),
        F.sum("contraction_mrr").alias("contraction_mrr"),
        F.sum("churned_mrr").alias("churned_mrr")
    )
    .orderBy("month")
)

# =========================================================
# Final outputs
# =========================================================
# 1) user_month_mrr_full       -> user-level current MRR by month
# 2) user_mrr_movement         -> user-level MRR movement including expansion
# 3) mrr_movement_by_month     -> company-level monthly totals

# COMMAND ----------

# MAGIC %md
# MAGIC ## active_subscriptions

# COMMAND ----------

active_subscriptions_by_month = (
    subscription_month_mrr
    .groupBy("month")
    .agg(
        F.countDistinct("subscription_id").alias("active_subscriptions")
    )
    .orderBy("month")
)

df2 = (
    df1
    .join(active_subscriptions_by_month, on="month", how="left")
    .fillna({"active_subscriptions": 0})
    .orderBy("month")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## trial_to_paid_conversions,

# COMMAND ----------

# MAGIC %md
# MAGIC subs + plans
# MAGIC → know product_id and whether the subscription is trial or paid
# MAGIC
# MAGIC payments
# MAGIC → know which subscription has successful payment
# MAGIC
# MAGIC Then:
# MAGIC paid subscription is convert_to_paid = 1
# MAGIC if the same user had a trial for the same product before that paid payment

# COMMAND ----------

# =========================================================
# Join subscriptions with plans
# This gives each subscription product_id, tier, price
# =========================================================
subs_plans = (
    subs.drop("price").alias("s")
    .join(
        plans.select(
            F.col("plan_id").alias("plan_join_id"),
            "product_id",
            "tier",
            "price"
        ).alias("p"),
        F.col("s.plan_id") == F.col("p.plan_join_id"),
        "left"
    )
    .drop("plan_join_id")
)

# =========================================================
# Create is_trial column
# =========================================================
subs_plans = (
    subs_plans
    .withColumn("tier_lc", F.lower(F.trim(F.col("tier"))))
    .withColumn(
        "is_trial",
        F.when(
            F.col("tier_lc").isin("trial", "free trial", "free_trial"),
            F.lit(1)
        ).when(
            F.col("price") == 0,
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

# =========================================================
# Get first successful payment for each subscription
# =========================================================
successful_payments = (
    payments
    .filter(F.col("payment_status") == "success")
    .filter(F.col("payment_date").isNotNull())
    .groupBy("subscription_id")
    .agg(
        F.min("payment_date").alias("first_success_payment_date")
    )
)

# =========================================================
# Join successful payment info back to subscriptions
# =========================================================
subs_full = (
    subs_plans
    .join(successful_payments, on="subscription_id", how="left")
)

# =========================================================
# Separate trial subscriptions
# =========================================================
trial_subs = (
    subs_full
    .filter(F.col("is_trial") == 1)
    .select(
        F.col("user_id").alias("trial_user_id"),
        F.col("product_id").alias("trial_product_id"),
        F.col("start_date").alias("trial_start_date"),
        F.col("end_date").alias("trial_end_date")
    )
)

# =========================================================
# Separate paid subscriptions with successful payment
# =========================================================
paid_subs = (
    subs_full
    .filter(F.col("is_trial") == 0)
    .filter(F.col("first_success_payment_date").isNotNull())
    .select(
        "subscription_id",
        "user_id",
        "product_id",
        "start_date",
        "first_success_payment_date"
    )
)

# =========================================================
# Check whether paid subscription had previous trial
# Same user + same product + payment after trial
# =========================================================
converted_paid_subs = (
    paid_subs.alias("paid")
    .join(
        trial_subs.alias("trial"),
        (
            (F.col("paid.user_id") == F.col("trial.trial_user_id")) &
            (F.col("paid.product_id") == F.col("trial.trial_product_id")) &
            (
                F.col("paid.first_success_payment_date") >= 
                F.coalesce(F.col("trial.trial_end_date"), F.col("trial.trial_start_date"))
            )
        ),
        "inner"
    )
    .select("paid.subscription_id")
    .distinct()
    .withColumn("convert_to_paid", F.lit(1))
)

# =========================================================
# Join result back to original subscription table
# =========================================================
subs_with_conversion = (
    subs_full
    .drop("convert_to_paid")
    .join(converted_paid_subs, on="subscription_id", how="left")
    .withColumn("convert_to_paid", F.coalesce(F.col("convert_to_paid"), F.lit(0)))
    .drop("tier_lc")
)

# COMMAND ----------

df3_temp = (
    subs_with_conversion
    .filter(F.col("convert_to_paid") == 1)
    .withColumn("month", F.date_trunc("month", F.col("first_success_payment_date")))
    .groupBy("month")
    .agg(
        F.countDistinct("subscription_id").alias("num_conversions")
    )
    .orderBy("month")
)

df3 = df2.join(df3_temp, on="month", how="left").fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC A renewal is a successful payment for an existing paid subscription after the initial paid payment. first successful payment = initial payment
# MAGIC second, third, fourth successful payments = renewals.
# MAGIC
# MAGIC We count the "reactivate" as renewal too, and successful payment after a trial is not renewal.
# MAGIC
# MAGIC => we group by user_id + product_id (because each product has many tiers), then, we will count, if there is 2 successful payment, then we count it as renewal.

# COMMAND ----------

df4_temp = payments.withColumn("month", F.date_trunc("month", F.col("payment_date")))\
  .filter(F.col("payment_status")=="success")\
  .select("subscription_id", "payment_status",
  "month").alias("p").join(
  subs.alias("s"),
  on="subscription_id",
  how="inner"
).join(
  plans.select("plan_id","product_id"),
  on="plan_id",
  how="inner"
).groupBy("user_id", "product_id").agg(
  F.count("payment_status").alias("num_payments"),
  F.collect_list("month").alias("month")
)

# COMMAND ----------

df4 = (
    df4_temp
    # sort the month list first, because collect_list does not guarantee order
    .withColumn("month_sorted", F.sort_array(F.col("month")))
    
    # remove the first element
    .withColumn(
        "renewal_months",
        F.slice(
            F.col("month_sorted"),
            2,
            F.size(F.col("month_sorted")) - 1
        )
    )
    
    # turn remaining months into rows
    .withColumn("month", F.explode(F.col("renewal_months")))
    
    # keep useful columns
    .select(
        "user_id",
        "product_id",
        "num_payments",
        "month"
    )
).filter(F.col("num_payments") > 1)\
.groupBy("month")\
.agg(F.countDistinct("user_id").alias("num_renewals"))\
.join(df3, on="month", how="right")\
.fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Churn count
# MAGIC
# MAGIC We just need to use a cutoff to give a row a label churn or not, and then, we just group by month.

# COMMAND ----------

df5_temp = (
    subs
    .withColumn(
        "churn",
        F.when(
            (
                F.lower(F.col("status")).isin("cancelled", "canceled", "expired")
            )
            |
            (
                F.col("end_date").isNotNull()
                # &
                # (F.to_date(F.col("end_date")) <= cutoff_date)
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn("month", F.date_trunc("month", F.col("end_date")))
    .filter(F.col("month").isNotNull())
    .groupBy("month")
    .agg(
        F.sum("churn").alias("num_churn")
    )
    .orderBy("month")
)

# COMMAND ----------

df5 = df5_temp.join(
    df4,
    on="month",
    how="left"
).fillna(0)

# COMMAND ----------

df6_temp = (
    changes
    .withColumn("month", F.date_trunc("month", F.col("change_date")))
    .groupBy("month")
    .agg(
        F.sum(F.when(F.lower(F.col("change_type")) == "upgrade", 1).otherwise(0)).alias("num_upgrades"),
        F.sum(F.when(F.lower(F.col("change_type")) == "downgrade", 1).otherwise(0)).alias("num_downgrades"),
        F.sum(F.when(F.lower(F.col("change_type"))=="initial", 1).otherwise(0)).alias("initial")
    )
    .orderBy("month")
)

# COMMAND ----------

df6 = df5.join(
    df6_temp,
    on="month",
    how="left"
).fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### enterprise_subscription_count
# MAGIC
# MAGIC Count all enterprise subscriptions in that month, no need to care whether or not trial (at this moment).
# MAGIC
# MAGIC Count for successful payments only.

# COMMAND ----------

df7_temp = subs.select("user_id", "subscription_id")\
  .join(payments.filter(F.col("payment_status")=="success").select("subscription_id", "payment_date"),
        on="subscription_id",how="inner")\
  .join(
    users.select("user_id", "is_enterprise"),
    on="user_id",
    how="left"
).withColumn("month", F.date_trunc("month", F.col("payment_date")))\
  .groupBy("month").agg(
    F.sum(F.when(F.col("is_enterprise")==True, 1).otherwise(0)).alias("num_enterprise")
  )

# COMMAND ----------

df7 = df6.join(
    df7_temp,
    on="month",
    how="left"
).fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## support_ticket_count
# MAGIC
# MAGIC Because there are month which are not in the df7, the total number of tickets might be less than total of tickets after join, this happens mostly because there are month in the tickets that do not exist in the df7 (error somewhere)

# COMMAND ----------

df8 = tickets.withColumn("month", F.date_trunc("month", F.col("created_at"))).\
    groupBy("month").agg(F.count("ticket_id").alias("num_tickets")).\
    join(df7, on="month", how="right").fillna(0)

# COMMAND ----------

display(df8)

# COMMAND ----------

# df8.coalesce(1).write \
#   .mode("overwrite") \
#   .option("header", True) \
#   .csv("/Volumes/datalake_catalog/datalake_schema/silver_draft/mrr")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename columns

# COMMAND ----------

w = Window.orderBy("date")
df9 = (df8
 .withColumnRenamed("current_mrr", "mrr")
 .withColumnRenamed("month", "date")
 .withColumnRenamed("num_tickets", "support_ticker_count")
 .withColumnRenamed("num_churn", "churned_subscriptions")
 .withColumnRenamed("num_renewals", "renewed_subscriptions")
 .withColumnRenamed("num_conversions", "trial_to_paid_conversions")
 .withColumnRenamed("num_upgrades", "upgrade_count")
 .withColumnRenamed("num_downgrades", "downgrade_count")
 .withColumnRenamed("initial", "new_subscriptions")
 .withColumn(
    "active_subscriptions_start_month",
    F.lag("active_subscriptions").over(w)
)
 .withColumn("year", F.year(F.col("date")))
 .withColumn("quarter", F.quarter(F.col("date")))
 .withColumn("arr", F.col("mrr")*12)
 .withColumn(
    "net_mrr_change",
    F.coalesce(F.col("new_mrr"), F.lit(0))
    + F.coalesce(F.col("expansion_mrr"), F.lit(0))
    - F.coalesce(F.col("contraction_mrr"), F.lit(0))
    - F.coalesce(F.col("churned_mrr"), F.lit(0)))
 .withColumn("previous_month_mrr", F.lag("mrr").over(w))
 .withColumn("mrr_growth_amount", F.col("mrr") - F.col("previous_month_mrr"))
 .withColumn("mrr_growth_rate", F.col("mrr_growth_amount")/F.col("previous_month_mrr"))
 .withColumn("churn_rate",
             F.when(
             F.col("active_subscriptions_start_month") > 0,
             (F.col("churned_subscriptions")/F.col("active_subscriptions_start_month")*100)).otherwise(F.lit(None)))
 .withColumn("retention_rate", 100- F.col("churn_rate"))
 .withColumn(
    "arpu",
    F.when(
        F.col("active_subscriptions") > 0,
        F.col("mrr") / F.col("active_subscriptions")
    ).otherwise(0))
 .withColumn(
    "nrr",
    F.when(
        (F.col("previous_month_mrr").isNotNull()) & (F.col("previous_month_mrr") > 0),
        (
            F.col("previous_month_mrr")
            + F.col("expansion_mrr")
            - F.col("churned_mrr")
            - F.col("contraction_mrr")
        ) / F.col("previous_month_mrr")
    ).otherwise(F.lit(None))))
 # previous_month_mrr is starting_mrr also

numeric_cols = [
    f.name for f in df9.schema.fields
    if isinstance(f.dataType, (DoubleType, FloatType, DecimalType))
]

df9 = df9.select(
    *[
        F.round(F.col(c), 2).alias(c) if c in numeric_cols else F.col(c)
        for c in df9.columns
    ]
)
# check columns
check_cols = ["date", "churn_rate", "churned_subscriptions", "active_subscriptions_start_month", "retention_rate"]
display(df9.select(*check_cols).orderBy("date"))

# COMMAND ----------

df_gold = df9.withColumn("saved_at", F.current_timestamp())

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("mock26.default.gold_monthly_mrr")

# COMMAND ----------

df_gold_read = spark.read.table("mock26.default.gold_monthly_mrr")