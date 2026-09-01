# Databricks notebook source
# Optional demo fixture for running the mocked Gold layer without production inputs.
from datetime import date
from pyspark.sql.types import *

CATALOG = "sellout_portfolio"
TARGET = f"{CATALOG}.silver.harmonized"

schema = StructType([
    StructField("date", DateType(), False),
    StructField("company", StringType(), False),
    StructField("company_type", StringType(), True),
    StructField("company_item_code", StringType(), True),
    StructField("item_code", StringType(), False),
    StructField("item_description", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("destination_country", StringType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("sales", DoubleType(), True),
    StructField("billing_currency", StringType(), True),
    StructField("local_currency", StringType(), True),
    StructField("sales_local_currency", DoubleType(), True),
    StructField("submission_id", LongType(), False),
])

rows = [
    (date(2025, 7, 1), "COMPANY_011", "manufacturer", "C011-SKU-001", "SKU-001", "Synthetic Product A", "CUSTOMER_001", "Wholesale", "Italy", 12.0, 1200.0, "EUR", "EUR", 1200.0, 1),
    (date(2025, 7, 1), "COMPANY_011", "manufacturer", "C011-SKU-001", "SKU-001", "Synthetic Product A", "CUSTOMER_001", "Wholesale", "Italy", 13.0, 1300.0, "EUR", "EUR", 1300.0, 2),
    (date(2025, 8, 1), "COMPANY_027", "manufacturer", "C027-SKU-002", "SKU-002", "Synthetic Product B", "CUSTOMER_002", "Retail", "Spain", 8.0, 920.0, "EUR", "EUR", 920.0, 1),
    (date(2025, 9, 1), "COMPANY_039", "dealer", "C039-SKU-003", "SKU-003", "Synthetic Product C", "CUSTOMER_003", "Retail", "France", 15.0, 1725.0, "EUR", "EUR", 1725.0, 1),
]

spark.createDataFrame(rows, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
print(f"Synthetic Gold demo input written to {TARGET}")
