# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %run ../common/portfolio_config

# COMMAND ----------
df = spark.table(GOLD_SELLOUT_TABLE)

checks = {
    "gold_not_empty": df.limit(1).count() > 0,
    "date_complete": df.filter(F.col("date").isNull()).limit(1).count() == 0,
    "company_complete": df.filter(F.col("company").isNull()).limit(1).count() == 0,
    "submission_complete": df.filter(F.col("submission_id").isNull()).limit(1).count() == 0,
    "single_submission_per_company_month": (
        df.groupBy("company", "reporting_month")
          .agg(F.countDistinct("submission_id").alias("versions"))
          .filter(F.col("versions") > 1)
          .limit(1).count() == 0
    ),
}

if "quantity" in df.columns:
    checks["quantity_non_negative"] = df.filter(F.col("quantity") < 0).limit(1).count() == 0

failed = [name for name, ok in checks.items() if not ok]
display(spark.createDataFrame(list(checks.items()), ["check_name", "passed"]))
if failed:
    raise ValueError(f"Gold data-quality checks failed: {failed}")
