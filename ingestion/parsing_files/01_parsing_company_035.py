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
import unicodedata


# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# =========================================
# CONFIGURATION
# =========================================

COMPANY_NAME = "company_035"

RAW_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.raw_ingestion"
TARGET_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.landing_company_035"

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
# MAGIC ## Processing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata from the file name (company, year, period) + country from inside the file
# MAGIC
# MAGIC `<company>_<country>_<year>_<period>`, e.g. `company_035_global_2026_q1.xlsx`. The
# MAGIC country token in the file name is just `global` (not a real country), so unlike the
# MAGIC other files, country is read from inside the sheet instead (`ESPAÑA`, row 1).

# COMMAND ----------

FILENAME_PATTERN = re.compile(
    r"^(?P<company>[a-z]+)_(?P<country>[a-z]+)_"
    r"(?P<year>20\d{2})_(?P<period>q[1-4]|h[1-2]|\d{2})$"
)

COMPANY_NAME_OVERRIDES = {"company_035": "company_035"}

COUNTRY_NAME_OVERRIDES = {"espana": "Spain"}

PERIOD_TYPE_BY_PREFIX = {"q": "quarterly", "h": "semi-annual"}


def _normalize(text: str) -> str:
    """Lowercase, ASCII-fold (e.g. ñ -> n) and strip everything but letters.

    Needed because the source file's accented characters (ñ, á, ó, ...) come through
    mis-encoded depending on how the cell is read; folding to plain ASCII sidesteps
    that entirely instead of trying to match the mis-encoded byte sequence.
    """
    decomposed = unicodedata.normalize("NFKD", str(text))
    ascii_only = decomposed.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", ascii_only.lower())


def parse_filename_metadata(file_path: str) -> dict:
    """Extract company, year and period from the file name.

    Expected pattern: <company>_<country>_<year>_<period>
    e.g. company_035_global_2026_q1.xlsx (the country token is ignored, see above)
    """
    stem = Path(file_path).stem.lower()
    match = FILENAME_PATTERN.match(stem)
    if not match:
        raise ValueError(f"Cannot parse metadata from file name: {file_path}")
    meta = match.groupdict()
    meta["company"] = COMPANY_NAME_OVERRIDES.get(meta["company"], meta["company"].title())
    meta["year"] = int(meta["year"])
    period = meta["period"].lower()
    meta["period_type"] = (
        "monthly" if period.isdigit() else PERIOD_TYPE_BY_PREFIX.get(period[0], period)
    )
    return meta


def period_start_date(year: int, period: str) -> pd.Timestamp:
    """First day of the reporting period (q1 -> Jan 1, h2 -> Jul 1, etc.)."""
    period = period.lower()
    if period.startswith("q"):
        month = {"q1": 1, "q2": 4, "q3": 7, "q4": 10}[period]
    elif period.startswith("h"):
        month = 1 if period == "h1" else 7
    else:
        month = int(period)
    return pd.Timestamp(year=year, month=month, day=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sheet structure
# MAGIC
# MAGIC Granularity is Year-Quarter-Product Category-Brand — there's no item code/SKU and no
# MAGIC monetary value here, only a shipped quantity per brand. The sheet is a small
# MAGIC hand-maintained pivot with 3 columns (`FAMILIA | SUB FAMILIA | <period>`):
# MAGIC
# MAGIC - `FAMILIA` (top-level product family, e.g. `EQUIPAMIENTO`) is filled only on the
# MAGIC   first row of each family, blank for every row below it until the next family starts
# MAGIC - `SUB FAMILIA` holds two different kinds of rows that look identical structurally
# MAGIC   (same 3 columns), and have to be told apart by position:
# MAGIC   - a **subcategory header** row (e.g. `Equipo dental (incluye carts)`), whose value
# MAGIC     is just the rollup total of the brand rows below it
# MAGIC   - the **brand** rows under it (e.g. `ANTHOS`, `MOCOM`, ...), which are the real
# MAGIC     leaf-level data points
# MAGIC - subcategories are padded with empty template rows (`MARCA N (rellenar con nombre
# MAGIC   de la marca)`) up to however many brand slots that subcategory was given — these
# MAGIC   carry no value and are skipped entirely
# MAGIC
# MAGIC A subcategory block normally ends at the next `MARCA N (...)` placeholder, but **not
# MAGIC always** — if every slot happens to be filled with a real brand, there's no leftover
# MAGIC placeholder to mark the boundary (e.g. `Radiográfico intraoral`, which has exactly 5
# MAGIC real brands and goes straight into the next subcategory with no placeholder in
# MAGIC between). To handle that, a running sum of the brand values seen so far under the
# MAGIC current subcategory is kept: once it reaches the subcategory's own recorded total, the
# MAGIC next row is treated as a new subcategory even without seeing a placeholder first.

# COMMAND ----------

PLACEHOLDER_RE = re.compile(r"^MARCA\s*\d+\s*\(rellenar con nombre de la marca\)$", re.IGNORECASE)

OUTPUT_COLUMNS = [
    "date", "company", "country",
    "item_description", "item_categories",
    "quantity", "period_type",
]


def parse_company_035_sheet(file_path: str, meta: dict) -> pd.DataFrame:
    """Parse the company_035 family/subfamily/brand pivot into long format, one row
    per (product category, brand)."""
    raw = pd.read_excel(file_path, sheet_name=0, header=None)

    country_raw = _normalize(raw.iat[1, 0])
    country = COUNTRY_NAME_OVERRIDES.get(country_raw, str(raw.iat[1, 0]).strip().title())

    header_idx = raw.index[raw[0] == "FAMILIA"][0]
    data = raw.iloc[header_idx + 1:].reset_index(drop=True)
    data.columns = ["familia", "text", "value"]

    date = period_start_date(meta["year"], meta["period"])
    records = []
    current_familia = None
    current_subfamilia = None
    current_total = None
    accumulated = 0.0
    expect_new = True  # the next non-placeholder row starts a new subcategory block

    for _, row in data.iterrows():
        familia, text, value = row["familia"], str(row["text"]).strip(), row["value"]

        if PLACEHOLDER_RE.match(text):
            expect_new = True
            continue

        if pd.notna(familia):
            current_familia = str(familia).strip()
            expect_new = True

        if not expect_new and pd.notna(current_total) and accumulated >= current_total:
            expect_new = True  # this subcategory's own total was already matched

        if expect_new:
            current_subfamilia = text
            current_total = value
            accumulated = 0.0
            expect_new = False
            continue

        if pd.notna(value):
            records.append({
                "date":             date,
                "company":          meta["company"],
                "country":          country,
                "item_description": text,
                "item_categories":  [current_familia, current_subfamilia],
                "quantity":         float(value),
                "period_type":      meta["period_type"],
            })
            accumulated += float(value)

    return pd.DataFrame.from_records(records)[OUTPUT_COLUMNS]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process the company_035 file

# COMMAND ----------

if df.count() == 0:

    print(F"No {COMPANY_NAME} files found in latest batch.")

else:

    rows = df.collect()

    frames = []

    for row in rows:

        file_path = row["file_path"]

        meta = parse_filename_metadata(file_path)

        print(f"Processing: {Path(file_path).name} (period_type={meta['period_type']})")
        
        frames.append(parse_company_035_sheet(file_path, meta))

    final_df = pd.concat(frames, ignore_index=True)

    final_df["date"] = final_df["date"].dt.date

    display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to Spark DataFrame
# MAGIC
# MAGIC Explicit schema so the types match what lands in the Databricks table — `date` as
# MAGIC `DateType` (it was reduced to `.dt.date` above, no time component). `spark` is
# MAGIC provided automatically in a Databricks notebook session.

# COMMAND ----------

company_035_schema = StructType([
    StructField("date",             DateType(),              True),
    StructField("company",          StringType(),            True),
    StructField("country",          StringType(),            True),
    StructField("item_description", StringType(),            True),
    StructField("item_categories",  ArrayType(StringType()), True),
    StructField("quantity",         DoubleType(),            True),
    StructField("period_type",      StringType(),            True),
])

# Reorder columns to match schema positional order (Spark assigns types by position, not name)
schema_col_names = [f.name for f in company_035_schema.fields]
final_df = final_df[schema_col_names]

spark_df = spark.createDataFrame(final_df, schema=company_035_schema)
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

print(f"✓ Appended to {TARGET_INGESTION_TABLE}")