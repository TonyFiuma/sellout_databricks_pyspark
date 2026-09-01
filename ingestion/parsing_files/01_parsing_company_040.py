# Databricks notebook source
# MAGIC %pip install "beautifulsoup4>=4.12,<5"

# COMMAND ----------

from pyspark.sql.functions import col, lit, expr,current_timestamp,regexp_extract
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import io 
import numpy as np
import warnings
import re
import email
from pathlib import Path
from bs4 import BeautifulSoup

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# =========================================
# CONFIGURATION
# =========================================

COMPANY_NAME = "company_040"

RAW_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.raw_ingestion"
TARGET_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.landing_company_040"

print(
    f"""
=========================================

{COMPANY_NAME} PROCESSING
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
# RESOLVE BATCH ID FOR JOB OR INTERACTIVE EXECUTION
# =========================================

try:
    manual_batch_id = dbutils.widgets.get("batch_id").strip()
except Exception:
    dbutils.widgets.text(
        "batch_id",
        "",
        "Batch ID (optional; empty uses latest company_040 batch)",
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
    batch_id_source = "latest company_040 batch"

print(f"Batch ID ({batch_id_source}): {batch_id}")

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
# MAGIC ### Processing

# COMMAND ----------

# ==========================================================
# FILE METADATA
# ==========================================================

FILENAME_PATTERN = re.compile(
    r"^(?P<company>[a-z]+)_(?P<country>[a-z]+)_"
    r"(?:(?P<channel>distributor|directsales)_)?"
    r"(?P<year>20\d{2})_(?P<period>q[1-4]|h[1-2]|\d{2})$"
)

CHANNEL_LABELS = {"distributor": "distributor", 
                  "directsales": "direct_sales"}


def parse_filename_metadata(file_path: str) -> dict:
    stem = Path(file_path).stem.lower()
    match = FILENAME_PATTERN.match(stem)
    if not match:
        raise ValueError(f"Cannot parse metadata from file name: {file_path}")
    meta = match.groupdict()
    meta["company"] = meta["company"].title()
    meta["country"] = meta["country"].title()
    meta["year"] = int(meta["year"])
    meta["channel"] = CHANNEL_LABELS.get(meta["channel"], meta["channel"])
    return meta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decode the .xls (it's actually a MHTML/SAP export) and read the data table
# MAGIC
# MAGIC It's parsed directly with BeautifulSoup instead of `pd.read_html` because the SAP report
# MAGIC displays numbers rounded to whole units (and sometimes scaled to thousands, e.g.
# MAGIC "* 1 000 PC"), but every `<td>` carries the exact value in its `x:num` attribute. If we
# MAGIC only parsed the visible text, low-volume rows would show as "-" (zero) when they actually
# MAGIC have a small non-zero value.

# COMMAND ----------

def _decode_mhtml(file_path: str) -> bytes:
    """Extract the raw HTML payload from a SAP-exported .xls (MHTML) file."""
    with open(file_path, "rb") as f:
        msg = email.message_from_bytes(f.read())
    return next(
        part.get_payload(decode=True)
        for part in msg.walk()
        if part.get_content_type() == "text/html"
    )


def _mhtml_table_to_frames(html_bytes: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the SAP HTML table into two aligned DataFrames: display text and exact x:num values.

    Manual grid reconstruction is required because the report header uses both:
    - colspan (e.g. "Overall Result" merges 4 columns into one cell)
    - rowspan (e.g. the empty top-left corner cell covers 4 columns x 4 rows)
    `pd.read_html` doesn't expose the x:num attribute (exact value) and also gets
    misaligned if rowspan isn't handled, since shorter rows get silently padded
    at the end instead of in the correct column.
    """
    soup = BeautifulSoup(html_bytes, "html.parser")
    tables = soup.find_all("table")
    # the real data table is always the largest one (the others are title/metadata/timestamp)
    target = max(tables, key=lambda t: len(t.find_all("tr")))

    active: dict[int, list] = {}  # col -> [rows remaining, text, x:num]
    text_grid, num_grid = [], []

    for tr in target.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        ci, col = 0, 0
        row_text, row_num = {}, {}
        while ci < len(cells) or col in active:
            if col in active:
                # this column slot is still occupied by a rowspan from a previous row
                remaining, txt, val = active[col]
                row_text[col], row_num[col] = txt, val
                if remaining - 1 > 0:
                    active[col] = [remaining - 1, txt, val]
                else:
                    del active[col]
                col += 1
                continue
            cell = cells[ci]
            ci += 1
            colspan = int(cell.get("colspan", 1))
            rowspan = int(cell.get("rowspan", 1))
            txt = cell.get_text(strip=True)
            num_attr = cell.get("x:num")
            val = float(num_attr) if num_attr is not None else np.nan
            for k in range(colspan):
                row_text[col + k] = txt
                row_num[col + k] = val
                if rowspan > 1:
                    active[col + k] = [rowspan - 1, txt, val]
            col += colspan
        width = max(row_text.keys(), default=-1) + 1
        text_grid.append([row_text.get(c, np.nan) for c in range(width)])
        num_grid.append([row_num.get(c, np.nan) for c in range(width)])

    max_width = max(len(r) for r in text_grid)
    text_grid = [r + [np.nan] * (max_width - len(r)) for r in text_grid]
    num_grid = [r + [np.nan] * (max_width - len(r)) for r in num_grid]
    return pd.DataFrame(text_grid), pd.DataFrame(num_grid)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamically detect the header rows
# MAGIC
# MAGIC The header always has 5 rows (metric / partner code / partner name / scenario / unit), but the order of
# MAGIC "partner code", "partner name" and "scenario (ACT 25)" differs between the
# MAGIC distributor file and the directsales file. Instead of assuming a fixed position,
# MAGIC each row's role is detected from its content.

# COMMAND ----------

def _is_scenario_row(row: pd.Series) -> bool:
    """True if most non-empty values look like a scenario/version tag (e.g. "ACT 25")."""
    vals = row.dropna()
    vals = vals[vals != ""]
    if vals.empty:
        return False
    return vals.str.match(r"^(ACT|BUD|PLAN)\s?\d{2}$").mean() > 0.5


def _numeric_fraction(row: pd.Series) -> float:
    """Fraction of non-empty values that are purely numeric (used to spot the partner-id row)."""
    vals = row.dropna()
    vals = vals[(vals != "") & (vals != "Partner") & (vals != "Overall Result")]
    if vals.empty:
        return 0.0
    return vals.str.fullmatch(r"\d+").mean()


def _unit_scale(unit_text) -> float:
    # some Quantity columns are labeled "* 1 000 PC": the underlying x:num value is
    # expressed in thousands, so it must be multiplied to get the real unit count
    return 1000.0 if re.search(r"1[\s\xa0]?000", str(unit_text)) else 1.0


def clean_item_code(value) -> str:
    """Remove an alphabetic suffix while preserving letters inside the item code."""
    return re.sub(r"[A-Za-z]+$", "", str(value).strip())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main parser
# MAGIC
# MAGIC - Keeps only "leaf" rows (a single specific product), never the category/subcategory
# MAGIC   subtotals nor the Result/Overall Result/~ROOT rows.
# MAGIC - The full category chain is tracked while scanning the source so that only leaf/product
# MAGIC   rows are emitted; the current landing schema does not persist those category labels.
# MAGIC - Each product row is exploded by partner: one output row per (product, customer),
# MAGIC   with `customer` taken from the report and the partner code included in `source_key`
# MAGIC   (the "Overall Result" column is dropped because it's the sum of the real customers).

# COMMAND ----------

TARGET_COLUMNS = [
    "date",
    "source_row_id",
    "source_key",
    "company_item_code",
    "company",
    "item_description",
    "brand_company",
    "item_code",
    "channel_raw",
    "customer_raw",
    "clean_customer",
    "country_raw",
    "destination_country_raw",
    "gross_sales_currency",
    "time_frames",
    "volumes",
    "quantity",
    "gross_sales",
    "data_matrix",
    "data_collection",
    "project",
    "file_name",
]


def parse_company_040_mhtml(file_path: str, meta: dict) -> pd.DataFrame:
    """Parse one company_040 SAP export (distributor or directsales) into long format,
    one row per (product, customer, month), using the landing_company_040 schema."""
    file_name = Path(file_path).name
    text_df, num_df = _mhtml_table_to_frames(_decode_mhtml(file_path))

    metric_row = text_df.iloc[0]  # "Gross Sales" / "Quantity" per column
    unit_row = text_df.iloc[4]    # "EUR" / "PC" / "* 1 000 PC" per column

    # rows 1-3 hold scenario / partner code / partner name, but in a different
    # order depending on the file, so each role is detected from its content
    header_candidates = [1, 2, 3]
    scenario_idx = next(i for i in header_candidates if _is_scenario_row(text_df.iloc[i]))
    remaining = [i for i in header_candidates if i != scenario_idx]
    code_idx = max(remaining, key=lambda i: _numeric_fraction(text_df.iloc[i]))
    name_idx = next(i for i in remaining if i != code_idx)
    code_row = text_df.iloc[code_idx]
    name_row = text_df.iloc[name_idx]

    scale = unit_row.apply(_unit_scale)
    num_df = num_df.mul(scale, axis=1)

    data_idx = text_df.index[5:]
    material = text_df[2].reindex(data_idx)
    description = text_df[3].reindex(data_idx)

    # hierarchy codes are pure digits, always an even length from 2 to 12
    # (category -> subcategory -> ... ); leaf/product codes break that pattern
    is_hierarchy = material.str.fullmatch(r"(\d{2}){1,6}").fillna(False)
    is_excluded = material.isin(["Result", "Overall Result", "~ROOT", ""]) | material.isna()
    is_leaf = ~is_hierarchy & ~is_excluded

    # category/subcategory chain: overwritten level by level as we walk down the
    # rows in order; a snapshot of the chain is taken whenever a leaf row is hit
    categories_by_row = {}
    chain = {}
    for idx in data_idx:
        code = material.at[idx]
        if is_hierarchy.at[idx]:
            level = len(code) // 2
            chain = {l: d for l, d in chain.items() if l < level}
            chain[level] = description.at[idx]
        elif is_leaf.at[idx]:
            categories_by_row[idx] = [chain[l] for l in sorted(chain)]

    value_cols = range(4, text_df.shape[1])
    gross_sales_cols = [c for c in value_cols if metric_row[c] == "Gross Sales"]
    quantity_cols = [c for c in value_cols if metric_row[c] == "Quantity"]

    # map each real partner (by id) to its Gross Sales and Quantity column index,
    # skipping the "Overall Result" pseudo-partner (it's the sum, not a customer)
    partners = {}
    for c in gross_sales_cols:
        pid, pname = code_row[c], name_row[c]
        if not pid or pid == "Overall Result" or pname == "Overall Result":
            continue
        partners.setdefault(pid, {"name": pname})["gs_col"] = c
    for c in quantity_cols:
        pid = code_row[c]
        if pid in partners:
            partners[pid]["qty_col"] = c

    records = []
    for idx in categories_by_row:
        item_code = clean_item_code(material.at[idx])
        item_description = description.at[idx].strip()
        month = int(text_df.at[idx, 0])
        date = pd.Timestamp(year=meta["year"], month=month, day=1).date()

        for pid, info in partners.items():
            gs_val = num_df.at[idx, info["gs_col"]] if "gs_col" in info else np.nan
            qty_val = num_df.at[idx, info["qty_col"]] if "qty_col" in info else np.nan
            if pd.isna(gs_val) and pd.isna(qty_val):
                continue
            records.append({
                "date": date,
                "source_row_id": int(idx),
                "source_key": (
                    f"{Path(file_name).stem}|{meta['channel']}|{idx}|{pid}"
                ),
                "company_item_code": f"{meta['company']}|{item_code}",
                "company": meta["company"],
                "item_description": item_description,
                "brand_company": "company_040 VIVADENT",
                "item_code": item_code,
                "channel_raw": meta["channel"],
                "customer_raw": info["name"],
                "clean_customer": None,
                "country_raw": None,
                "destination_country_raw": meta["country"],
                "gross_sales_currency": "EUR",
                "time_frames": "month",
                "volumes": np.nan,
                "quantity": float(qty_val) if pd.notna(qty_val) else 0.0,
                "gross_sales": float(gs_val) if pd.notna(gs_val) else 0.0,
                "data_matrix": None,
                "data_collection": "sell-out",
                "project": "sell-out",
                "file_name": file_name,
            })

    df = pd.DataFrame.from_records(records, columns=TARGET_COLUMNS)

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").dt.date
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process both company_040 files (distributor + direct sales)

# COMMAND ----------

if df.count() == 0:

    print("No company_040 files found in latest batch.")
    dbutils.notebook.exit("No company_040 files found in latest batch.")

else:

    rows = df.collect()

    frames = []

    for row in rows:

        file_path = row["file_path"]

        meta = parse_filename_metadata(file_path)

        print(f"Processing: {Path(file_path).name}  (channel={meta['channel']})")
        
        frames.append(parse_company_040_mhtml(file_path, meta))

    final_df = pd.concat(frames, ignore_index=True)

    if final_df.empty:
        dbutils.notebook.exit("No company_040 rows were produced from the current batch.")

    display(final_df)

# COMMAND ----------

## Schema

company_040_schema = StructType([
    StructField("date", DateType(), True),
    StructField("source_row_id", IntegerType(), True),
    StructField("source_key", StringType(), True),
    StructField("company_item_code", StringType(), True),
    StructField("company", StringType(), True),
    StructField("item_description", StringType(), True),
    StructField("brand_company", StringType(), True),
    StructField("item_code", StringType(), True),
    StructField("channel_raw", StringType(), True),
    StructField("customer_raw", StringType(), True),
    StructField("clean_customer", StringType(), True),
    StructField("country_raw", StringType(), True),
    StructField("destination_country_raw", StringType(), True),
    StructField("gross_sales_currency", StringType(), True),
    StructField("time_frames", StringType(), True),
    StructField("volumes", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("gross_sales", DoubleType(), True),
    StructField("data_matrix", StringType(), True),
    StructField("data_collection", StringType(), True),
    StructField("project", StringType(), True),
    StructField("file_name", StringType(), True),
])

final_df = final_df[TARGET_COLUMNS]
spark_df = spark.createDataFrame(final_df, schema=company_040_schema)

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
        "company_040 output schema does not match landing_company_040. "
        f"Output: {output_signature}; target: {target_signature}"
    )

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

print(f"Appended to {TARGET_INGESTION_TABLE}")

