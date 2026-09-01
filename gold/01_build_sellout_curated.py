# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %run ../common/portfolio_config

# COMMAND ----------
# Gold business-serving rule (portfolio extension):
# publish only the latest submission received for each company/reporting month.

silver = spark.table(HARMONIZED_TABLE)

required = {"date", "company", "submission_id"}
missing = required.difference(silver.columns)
if missing:
    raise ValueError(f"Gold input is missing required Silver columns: {sorted(missing)}")

reporting_month = F.to_date(F.date_trunc("month", F.col("date")))
latest_window = Window.partitionBy("company", reporting_month)

curated = (
    silver
    .withColumn("reporting_month", reporting_month)
    .withColumn("_latest_submission_id", F.max("submission_id").over(latest_window))
    .filter(F.col("submission_id") == F.col("_latest_submission_id"))
    .drop("_latest_submission_id")
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("semester", F.when(F.month("date") <= 6, "H1").otherwise("H2"))
    .withColumn("gold_loaded_at", F.current_timestamp())
)

# Preserve the rich Silver business columns instead of remodelling them into artificial
# dimensions/facts. Add a useful derived KPI only when both measures exist.
if {"sales_local_currency", "quantity"}.issubset(curated.columns):
    curated = curated.withColumn(
        "sales_per_unit",
        F.when(
            F.col("quantity") > 0,
            F.round(F.col("sales_local_currency") / F.col("quantity"), 2),
        ),
    )

curated.createOrReplaceTempView("_portfolio_sellout_curated")

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_SELLOUT_TABLE}
    CLUSTER BY AUTO
    COMMENT 'Synthetic/pseudonymized business-ready Sell-Out portfolio dataset'
    AS SELECT * FROM _portfolio_sellout_curated
""")
