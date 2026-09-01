# Databricks notebook source
from pyspark.sql.functions import col, lit, expr,current_timestamp,regexp_extract
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import io 
import numpy as np
import re
from io import BytesIO
from pathlib import Path
from collections import defaultdict
import warnings

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# =========================================
# CONFIGURATION
# =========================================

COMPANY_NAME = "company_011"

RAW_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.raw_ingestion"
TARGET_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.landing_company_011"

print(
    f"""
=========================================

COMPANY_NAME PROCESSING
Raw Ingestion Table : {RAW_INGESTION_TABLE}
Target Ingestion Table : {TARGET_INGESTION_TABLE}

=========================================
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read last files ingestion

# COMMAND ----------

# =========================================
# GET BATCH ID FROM PREVIOUS TASK
# =========================================

batch_id = dbutils.jobs.taskValues.get(
    taskKey="00_ingestion_raw_files",
    key="batch_id"
)

print(f"Batch ID: {batch_id}")

# COMMAND ----------

print(f"Processing company: {COMPANY_NAME}")
print(f"Batch ID: {batch_id}")

# =========================================
# READ FILES FROM CURRENT BATCH
# =========================================
df = (
    spark.table(RAW_INGESTION_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .filter(F.col("company_name") == COMPANY_NAME)
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing logic — structure & decisions
# MAGIC
# MAGIC ### Source file
# MAGIC Flat transactional SAP export (`Sheet1`). One row = one line item (material × customer × month).
# MAGIC Multi-country: 16 countries covered under a single global file.
# MAGIC
# MAGIC ### Derived columns (enriched from raw data)
# MAGIC | Output column | Source | Decision |
# MAGIC |---|---|---|
# MAGIC | `date` | `Month` (format `MM.YYYY`) | Fallback to `MONTH 2` + `YEAR` for ~7.5k rows from 2020 where Excel stored the value as a truncated float (e.g. `1.202` instead of `01.2020`) |
# MAGIC | `country` | `COUNTRY` | Title-cased; the filename says "global" so country is read from inside the file |
# MAGIC | `channel` | `Cust.Acct.Assg.Group` | `"00 Domestic Trade Sales"` → Direct Sales · `"02 Export Trade Sales"` → Distribution |
# MAGIC | `item_code` | `Material` (first token) | SAP Material field format: `"9560281  EXTENSION ARM 188CM REACH"` |
# MAGIC | `item_description` | `Material` (rest after first token) | Same field, split on first whitespace |
# MAGIC | `customer_code` | `Sold-to party` (leading digits) | SAP format: `"8909521  JLC - ASSIST. TECNICA E VENDA"` |
# MAGIC | `customer` | `Sold-to party` (rest after digits) | Same field |
# MAGIC | `item_categories` | `KEY PRODUCT` + `PRODUCT PLANING` | Two-level product hierarchy kept as a list `[key_product, product_planning]` |
# MAGIC
# MAGIC ### Columns dropped as redundant
# MAGIC - `Month`, `MONTH 2`, `YEAR`, `QUARTER` — fully represented by `date`
# MAGIC - `Material` — split into `item_code` + `item_description`
# MAGIC - `Sold-to party` — split into `customer_code` + `customer`
# MAGIC - `KEY PRODUCT`, `PRODUCT PLANING` — merged into `item_categories`
# MAGIC - `COUNTRY` — cleaned and kept as `country`
# MAGIC - `Unnamed: 27/28/29` — empty / row counter / quarter duplicate
# MAGIC
# MAGIC ### Remaining raw SAP columns (renamed to snake_case)
# MAGIC `sales_organization`, `sales_office`, `customer_account_assignment_group`, `business_unit`, `line_of_business`, `pac1`, `pac23`, `gross_sales`, `gross_sales_currency`, `sales_packet`, `sales_packet_currency`, `quantity`, `quantity_uom`, `country_code`, `gross_avg_price`, `gross_avg_price_currency`, `packet_avg_price`, `packet_avg_price_currency`
# MAGIC

# COMMAND ----------

CHANNEL_MAP = {
    "00 Domestic Trade Sales": "direct_sales",
    "02 Export Trade Sales":   "distribution",
}

# Columns to drop — already captured in derived fields (drop before rename, so use original names)
_DROP_COLS = [
    "Month", "MONTH 2", "YEAR",          # → date
    "COUNTRY",                            # → country
    "Sold-to party",                      # → customer_code + customer
    "Material",                           # → item_code + item_description
    "QUARTER",                            # → derivable from date
    "KEY PRODUCT", "PRODUCT PLANING",     # → item_categories
    "Unnamed: 27", "Unnamed: 28", "Unnamed: 29",
]

_RAW_RENAME = {
    "Sales Organization":    "sales_organization",
    "Sales Office":          "sales_office",
    "Cust.Acct.Assg.Group":  "customer_account_assignment_group",
    "Business Unit":         "business_unit",
    "Line of Business":      "line_of_business",
    "PAC1":                  "pac1",
    "PAC23":                 "pac23",
    "SLS: Gross":            "gross_sales",
    "SLS: Gross_2_":         "gross_sales_currency",
    "SLS:Pkt Sl":            "sales_packet",
    "SLS:Pkt Sl_2_":         "sales_packet_currency",
    "SLS: Qty":              "quantity",
    "SLS: Qty_2_":           "quantity_uom",
    "CtrySotoPa":            "country_code",
    "SLS:GrAvPr":            "gross_avg_price",
    "SLS:GrAvPr_2_":         "gross_avg_price_currency",
    "SLS:PkAvPr":            "packet_avg_price",
    "SLS:PkAvPr_2_":         "packet_avg_price_currency",
}

def parse_company_011_excel(file_path: str, file_name: str) -> pd.DataFrame:

    df = pd.read_excel(file_path)

    # DATE: try "MM.YYYY"; fall back to MONTH 2 + YEAR for truncated 2020 values (e.g. "1.202")
    df["date"] = pd.to_datetime(df["Month"].astype(str), format="%m.%Y", errors="coerce")
    mask = df["date"].isna()
    if mask.any():
        df.loc[mask, "date"] = pd.to_datetime(
            df.loc[mask, "YEAR"].astype(int).astype(str)
            + "-"
            + df.loc[mask, "MONTH 2"].astype(int).astype(str).str.zfill(2)
            + "-01"
        )

    # DERIVED COLUMNS
    df["item_code"]        = df["Material"].astype(str).str.strip().str.split().str[0]
    df["item_description"] = df["Material"].astype(str).str.replace(r"^\S+\s+", "", regex=True).str.strip()
    df["customer_code"]    = df["Sold-to party"].astype(str).str.extract(r"^(\d+)", expand=False)
    df["customer"]         = df["Sold-to party"].astype(str).str.replace(r"^\d+\s+", "", regex=True).str.strip()
    df["country"]          = df["COUNTRY"].astype(str).str.strip().str.title()
    df["channel"]          = df["Cust.Acct.Assg.Group"].map(CHANNEL_MAP)
    df["item_categories"]  = df.apply(
        lambda r: [str(r["KEY PRODUCT"]).strip(), str(r["PRODUCT PLANING"]).strip()], axis=1
    )
    df["company"] = "company_011"

    # Drop source columns already captured in derived fields
    df = df.drop(columns=[c for c in _DROP_COLS if c in df.columns])

    # Rename remaining raw columns to snake_case
    df = df.rename(columns=_RAW_RENAME)

    # Derived columns first, then raw SAP columns in original file order
    derived = [
        "date", "company", "country", "channel",
        "item_code", "item_description", "item_categories",
        "customer_code", "customer",
    ]
    rest = [c for c in df.columns if c not in derived]
    return df[derived + rest]


# COMMAND ----------

# ==========================================================
# PROCESS INPUT FILES
# ==========================================================

# Check whether the latest batch contains any files
if df.count() == 0:

    print("No company_011 files found in the latest batch.")

else:

    print(f"Files found: {df.count()}")

    rows = df.collect()
    dataframes = []

    for row in rows:
        df_tmp = parse_company_011_excel(
            file_path=row.file_path,
            file_name=row.file_name,
        )
        dataframes.append(df_tmp)

    final_df = pd.concat(dataframes, ignore_index=True)

    for col in ["gross_sales", "sales_packet", "gross_avg_price", "packet_avg_price"]:
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce").round(2)

    final_df["quantity"] = pd.to_numeric(final_df["quantity"], errors="coerce").fillna(0).astype(int)
    final_df["date"]     = final_df["date"].dt.date

    print(f"Total rows:  {len(final_df)}")
    print(f"Null dates:  {final_df['date'].isna().sum()}")
    print(f"Countries:   {sorted(final_df['country'].unique())}")
    print(f"Channels:    {final_df['channel'].value_counts().to_dict()}")
    print(f"Columns:     {list(final_df.columns)}")

# COMMAND ----------

final_df.info()

# COMMAND ----------

company_011_schema = StructType([
    # --- Derived / enriched columns ---
    StructField("date",                              DateType(),              True),
    StructField("company",                           StringType(),            True),
    StructField("country",                           StringType(),            True),
    StructField("channel",                           StringType(),            True),
    StructField("item_code",                         StringType(),            True),
    StructField("item_description",                  StringType(),            True),
    StructField("item_categories",                   ArrayType(StringType()), True),
    StructField("customer_code",                     StringType(),            True),
    StructField("customer",                          StringType(),            True),
    # --- Raw SAP columns in original file order ---
    StructField("sales_organization",                StringType(),            True),
    StructField("sales_office",                      StringType(),            True),
    StructField("customer_account_assignment_group", StringType(),            True),
    StructField("business_unit",                     StringType(),            True),
    StructField("line_of_business",                  StringType(),            True),
    StructField("pac1",                              StringType(),            True),
    StructField("pac23",                             StringType(),            True),
    StructField("gross_sales",                       DoubleType(),            True),
    StructField("gross_sales_currency",              StringType(),            True),
    StructField("sales_packet",                      DoubleType(),            True),
    StructField("sales_packet_currency",             StringType(),            True),
    StructField("quantity",                          IntegerType(),           True),
    StructField("quantity_uom",                      StringType(),            True),
    StructField("country_code",                      StringType(),            True),
    StructField("gross_avg_price",                   DoubleType(),            True),
    StructField("gross_avg_price_currency",          StringType(),            True),
    StructField("packet_avg_price",                  DoubleType(),            True),
    StructField("packet_avg_price_currency",         StringType(),            True),
])

spark_df = spark.createDataFrame(
    final_df,
    schema=company_011_schema
)

# COMMAND ----------

spark_df.display()

# COMMAND ----------

# spark.sql(f"""
# DROP TABLE IF EXISTS {TARGET_INGESTION_TABLE}
# """)

# COMMAND ----------

# append to bronze table
(
    spark_df.write
         .format("delta")
         .mode("append")
         .saveAsTable(TARGET_INGESTION_TABLE)
)

print(f"✓ Appended to {TARGET_INGESTION_TABLE}")