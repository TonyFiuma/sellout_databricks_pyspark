# Databricks notebook source
# MAGIC %pip install "openpyxl>=3.1,<4"

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import re
import unicodedata
from pathlib import Path
import warnings


# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

COMPANY_NAME = "company_039"
OUTPUT_COMPANY_NAME = "company_039"

RAW_INGESTION_TABLE    = f"{CATALOG}.{BRONZE_SCHEMA}.raw_ingestion"
TARGET_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.landing_company_039"

print(f"""
=========================================
{COMPANY_NAME.upper()} PROCESSING
Raw Ingestion Table    : {RAW_INGESTION_TABLE}
Target Ingestion Table : {TARGET_INGESTION_TABLE}
=========================================
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read last files ingestion

# COMMAND ----------

# =========================================
# RESOLVE BATCH ID FOR JOB OR INTERACTIVE EXECUTION
# =========================================

try:
    manual_batch_id = dbutils.widgets.get("batch_id").strip()
except Exception:
    dbutils.widgets.text(
        "batch_id",
        "",
        "Batch ID (optional; empty uses latest company_039 batch)",
    )
    manual_batch_id = dbutils.widgets.get("batch_id").strip()

try:
    job_batch_id = dbutils.jobs.taskValues.get(
        taskKey="00_ingestion_raw_files",
        key="batch_id",
        debugValue="",
    )
except ValueError:
    job_batch_id = ""

if manual_batch_id:
    batch_id = manual_batch_id
    batch_id_source = "widget parameter"
elif job_batch_id:
    batch_id = job_batch_id
    batch_id_source = "previous job task"
else:
    latest_batch = (
        spark.table(RAW_INGESTION_TABLE)
        .filter(F.lower(F.col("company_name")) == COMPANY_NAME.lower())
        .filter(F.col("batch_id").isNotNull())
        .orderBy(F.col("ingestion_time").desc())
        .select("batch_id")
        .first()
    )
    if latest_batch is None:
        raise ValueError(
            f"No batch found in {RAW_INGESTION_TABLE} for company {COMPANY_NAME}"
        )
    batch_id = latest_batch["batch_id"]
    batch_id_source = "latest company_039 batch"

print(f"Batch ID ({batch_id_source}): {batch_id}")


# COMMAND ----------

print(f"Processing company: {COMPANY_NAME}")
print(f"Batch ID: {batch_id}")

df = (
    spark.table(RAW_INGESTION_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .filter(F.lower(F.col("company_name")) == COMPANY_NAME.lower())
)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing logic — structure & decisions
# MAGIC
# MAGIC ### Source file
# MAGIC Flat transactional export from company_039's internal system. Single sheet `iventur`.
# MAGIC One row = one line item (product × customer segment × transaction date).
# MAGIC The file covers a single country (Spain) and a single month derived from the filename token `04` → April.
# MAGIC
# MAGIC ### File naming convention
# MAGIC `company_039_{scope}_{year}_{month}.xlsx`
# MAGIC - `scope` = `global` means country must be read from inside the file (`Nombre` column)
# MAGIC - `year` + `month` → used only as fallback; the `Fecha` column provides the exact transaction date
# MAGIC
# MAGIC ### Derived columns
# MAGIC | Output column | Source | Decision |
# MAGIC |---|---|---|
# MAGIC | `date` | `Fecha` (e.g. `"Apr 17 2026 12:00:00:000AM"`) | Parse first 3 tokens (month, day, year); discard the time component |
# MAGIC | `company` | constant | `company_039`, matching the legacy company name |
# MAGIC | `company_item_code` | `company` + `CodVentur` | Pipe-delimited company and item code |
# MAGIC | `item_code` | `CodVentur` | Internal company_039 product code |
# MAGIC | `product_family_level_1` | `Descripcion` | Product family |
# MAGIC | `manufacturer_item_code` | `CodFabricante` | Manufacturer product code |
# MAGIC | `brand_company` | `Marca` | Product brand |
# MAGIC | `item_description` | `Producto` | Product name with whitespace normalized |
# MAGIC | `customer_raw` | `Nome cliente` | Customer segment supplied by company_039 |
# MAGIC | `country_raw` | `Nombre` | Country name normalized to English |
# MAGIC | `destination_country_raw` | `country_raw` | Same country used as sale destination |
# MAGIC | `quantity` | `Unidades` | Always integer units |
# MAGIC | `gross_sales` | `Unidades × Precio` | Net line value before sales charge |
# MAGIC | `sales_charge` | `Cargo` | Charge rate applied to the line |
# MAGIC | `gross_sales_with_charges` | `gross_sales × (1 + Cargo)` | Line value including charges |
# MAGIC

# COMMAND ----------

COUNTRY_NAME_OVERRIDES = {
    "espana": "Spain",
}

TARGET_COLUMNS = [
    "date",
    "source_row_id",
    "source_key",
    "company_item_code",
    "company",
    "item_code",
    "product_family_level_1",
    "manufacturer_item_code",
    "brand_company",
    "item_description",
    "customer_raw",
    "postal_code",
    "locality",
    "province_code",
    "province",
    "country_code",
    "country_raw",
    "destination_country_raw",
    "series",
    "document_number",
    "product_line",
    "price_list",
    "clean_customer",
    "channel_raw",
    "gross_sales",
    "data_matrix",
    "data_collection",
    "project",
    "file_name",
    "sales_charge",
    "gross_sales_with_charges",
    "time_frames",
    "volumes",
    "quantity",
]


def _normalize_country(text: str) -> str:
    """Fold accented characters to ASCII for dictionary lookup (e.g. ñ → n)."""
    decomposed = unicodedata.normalize("NFKD", str(text))
    ascii_only = decomposed.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", ascii_only.lower())


def _clean_text(value) -> str | None:
    if pd.isna(value):
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def _clean_identifier(value) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)) and value.is_integer():
        return str(int(value))
    cleaned = str(value).strip()
    return cleaned or None


def parse_company_039_excel(file_path: str, file_name: str) -> pd.DataFrame:

    source = pd.read_excel(file_path, sheet_name=0)
    source_row_ids = np.arange(2, len(source) + 2, dtype=np.int32)
    file_stem = Path(file_name).stem

    date = pd.to_datetime(
        source["Fecha"].astype(str).str.split().str[:3].str.join(" "),
        errors="coerce",
    ).dt.date

    country = source["Nombre"].apply(
        lambda v: COUNTRY_NAME_OVERRIDES.get(_normalize_country(v), str(v).strip().title())
    )

    item_code = source["CodVentur"].map(_clean_identifier)
    quantity = pd.to_numeric(source["Unidades"], errors="coerce").fillna(0.0)
    net_unit_price = pd.to_numeric(source["Precio"], errors="coerce").fillna(0.0)
    gross_sales = (quantity * net_unit_price).round(2)
    sales_charge = pd.to_numeric(source["Cargo"], errors="coerce").fillna(0.0)

    result = pd.DataFrame({
        "date": date,
        "source_row_id": source_row_ids,
        "source_key": [
            f"{file_stem}|{source_row_id}"
            for source_row_id in source_row_ids
        ],
        "company_item_code": OUTPUT_COMPANY_NAME + "|" + item_code,
        "company": OUTPUT_COMPANY_NAME,
        "item_code": item_code,
        "product_family_level_1": source["Descripcion"].map(_clean_text),
        "manufacturer_item_code": source["CodFabricante"].map(_clean_identifier),
        "brand_company": source["Marca"].map(_clean_text),
        "item_description": source["Producto"].map(_clean_text),
        "customer_raw": source["Nome cliente"].map(_clean_text),
        "postal_code": source["CPostal"].map(_clean_identifier),
        "locality": source["Localida"].map(_clean_text),
        "province_code": source["CodProvincia"].map(_clean_identifier),
        "province": source["Provincia"].map(_clean_text),
        "country_code": source["CodPais"].map(_clean_identifier),
        "country_raw": country,
        "destination_country_raw": country,
        "series": source["Serie"].map(_clean_identifier),
        "document_number": source["Document"].map(_clean_identifier),
        "product_line": source["Linea"].map(_clean_identifier),
        "price_list": source["Tarifa (company_039)"].map(_clean_text),
        "clean_customer": None,
        "channel_raw": None,
        "gross_sales": gross_sales,
        "data_matrix": None,
        "data_collection": "sell-out",
        "project": "sell-out",
        "file_name": Path(file_name).name,
        "sales_charge": sales_charge,
        "gross_sales_with_charges": gross_sales * (1.0 + sales_charge),
        "time_frames": "month",
        "volumes": np.nan,
        "quantity": quantity.astype(float),
    })
    return result[TARGET_COLUMNS]


# COMMAND ----------

# ==========================================================
# PROCESS INPUT FILES
# ==========================================================

if df.count() == 0:

    print("No company_039 files found in the latest batch.")
    dbutils.notebook.exit("No company_039 files found in the latest batch.")

else:

    print(f"Files found: {df.count()}")

    rows = df.collect()
    dataframes = []

    for row in rows:
        df_tmp = parse_company_039_excel(
            file_path=row.file_path,
            file_name=row.file_name,
        )
        dataframes.append(df_tmp)

    final_df = pd.concat(dataframes, ignore_index=True)

    if final_df.empty:
        dbutils.notebook.exit("No company_039 rows were produced from the current batch.")

    print(f"Total rows:  {len(final_df)}")
    print(f"Null dates:  {final_df['date'].isna().sum()}")
    print(f"Countries:   {sorted(final_df['country_raw'].unique())}")
    print(f"Customers:   {sorted(final_df['customer_raw'].unique())}")
    print(f"Columns:     {list(final_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert to Spark DataFrame

# COMMAND ----------

company_039_schema = StructType([
    StructField("date", DateType(), True),
    StructField("source_row_id", IntegerType(), True),
    StructField("source_key", StringType(), True),
    StructField("company_item_code", StringType(), True),
    StructField("company", StringType(), True),
    StructField("item_code", StringType(), True),
    StructField("product_family_level_1", StringType(), True),
    StructField("manufacturer_item_code", StringType(), True),
    StructField("brand_company", StringType(), True),
    StructField("item_description", StringType(), True),
    StructField("customer_raw", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("province_code", StringType(), True),
    StructField("province", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country_raw", StringType(), True),
    StructField("destination_country_raw", StringType(), True),
    StructField("series", StringType(), True),
    StructField("document_number", StringType(), True),
    StructField("product_line", StringType(), True),
    StructField("price_list", StringType(), True),
    StructField("clean_customer", StringType(), True),
    StructField("channel_raw", StringType(), True),
    StructField("gross_sales", DoubleType(), True),
    StructField("data_matrix", StringType(), True),
    StructField("data_collection", StringType(), True),
    StructField("project", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("sales_charge", DoubleType(), True),
    StructField("gross_sales_with_charges", DoubleType(), True),
    StructField("time_frames", StringType(), True),
    StructField("volumes", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
])

final_df = final_df[TARGET_COLUMNS]
spark_df = spark.createDataFrame(final_df, schema=company_039_schema)

# Tag every row with the current batch for incremental downstream processing
spark_df = spark_df.withColumn("batch_id", F.lit(batch_id))

# Schema validation (ignore batch_id since it may not exist in target yet)
output_signature = [
    (field.name, field.dataType.simpleString())
    for field in spark_df.schema.fields
    if field.name != "batch_id"
]
target_signature = [
    (field.name, field.dataType.simpleString())
    for field in spark.table(TARGET_INGESTION_TABLE).schema.fields
    if field.name != "batch_id"
]
if output_signature != target_signature:
    raise ValueError(
        "company_039 output schema does not match landing_company_039. "
        f"Output: {output_signature}; target: {target_signature}"
    )

spark_df.display()


# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {TARGET_INGESTION_TABLE}")


# COMMAND ----------

# append to bronze table
(
    spark_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(TARGET_INGESTION_TABLE)
)

print(f"Appended to {TARGET_INGESTION_TABLE}")

