# Databricks notebook source
import uuid
from pyspark.sql.functions import (
    lit,
    col,
    current_timestamp,
    regexp_extract,
    length
)
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

TABLE = "raw_ingestion"

TARGET_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE}"

print(
    f"""
=========================================
Starting file ingestion

Source Path : {RAW_FILES_PATH}
Target Table: {TARGET_TABLE}

=========================================
"""
)

# COMMAND ----------

print("Cleaning test environment...")

dbutils.fs.rm(
    RAW_INGESTION_CHECKPOINT,
    True
)

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

print(f"Checkpoint deleted: {RAW_INGESTION_CHECKPOINT}")
print(f"Table dropped: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Files ingestion

# COMMAND ----------

batch_id = str(uuid.uuid4())

print(
    f"""
=========================================
Starting file ingestion

Batch ID     : {batch_id}
Source Path  : {RAW_FILES_PATH}
Target Table : {TARGET_TABLE}
Checkpoint   : {RAW_INGESTION_CHECKPOINT}

=========================================
"""
)

query = (
    spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", FILE_FORMAT)
        .load(RAW_FILES_PATH)
        .withColumn("batch_id", lit(batch_id))
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("file_size", col("_metadata.file_size"))
        .withColumn(
            "company_name",
            regexp_extract(col("file_name"), r"^([^_]+)", 1)
        )
        .withColumn(
            "file_path",
            F.regexp_replace(col("path"), "^dbfs:", "")
        )
        .withColumn("file_extension", F.element_at(F.split(col("file_path"), r"\."), -1))
        .drop("content")
        .writeStream
        .trigger(once=TRIGGER_ONCE)
        .option("checkpointLocation", RAW_INGESTION_CHECKPOINT)
        .toTable(TARGET_TABLE)
)

query.awaitTermination()

dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)

print(f"Ingestion completed. Batch ID: {batch_id}")

# COMMAND ----------

# =========================================
# READ CURRENT BATCH
# =========================================

df_files = (
    spark.table(TARGET_TABLE)
    .filter(col("batch_id") == batch_id)
)

print(f"Files in batch: {df_files.count()}")

# =========================================
# GET DISTINCT COMPANIES
# =========================================

companies = (
    df_files
    .select("company_name")
    .distinct()
    .collect()
)

print("Companies in batch:")

for row in companies:
    print(f"- {row['company_name']}")

# COMMAND ----------

display(
    spark.sql(
        f"SELECT * FROM {TARGET_TABLE} LIMIT 10"
    )
)