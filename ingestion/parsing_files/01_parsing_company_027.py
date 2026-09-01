# Databricks notebook source
# MAGIC %pip install "xlrd>=2.0,<3"

# COMMAND ----------

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

COMPANY_NAME = "company_027"

RAW_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.raw_ingestion"
TARGET_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.landing_company_027"

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
# RESOLVE BATCH ID FOR JOB OR INTERACTIVE EXECUTION
# =========================================

try:
    manual_batch_id = dbutils.widgets.get("batch_id").strip()
except Exception:
    dbutils.widgets.text(
        "batch_id",
        "",
        "Batch ID (optional; empty uses latest company_027 batch)",
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
    batch_id_source = "latest company_027 batch"

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

def parse_company_027_excel(file_path: str, file_name: str) -> pd.DataFrame:

    df = pd.read_excel(file_path)

    # Standardize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Drop subtotal/total rows
    df = df[
        ~df.astype(str)
        .apply(lambda row: row.str.lower().str.contains("total", na=False).any(), axis=1)
    ]

    # Extract item_code (CIC) from trailing parentheses in articolo: "PRODUCT NAME (270432)" → "270432"
    def extract_cic(articolo):
        match = re.search(r"\(([^()]*)\)\s*$", str(articolo))
        return match.group(1).strip() if match else "_"

    df["cic"] = df["articolo"].apply(extract_cic)

    # Remove trailing (CIC) from articolo, then clean whitespace
    df["articolo"] = (
        df["articolo"]
        .astype(str)
        .str.replace(r"\s*\([^()]*\)\s*$", "", regex=True)
        .str.strip()
        .str.replace(r" {2,}", " ", regex=True)
    )

    # Remove all spaces from item_code (CIC codes must not have internal spaces)
    df["cic"] = (
        df["cic"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
    )

    # Metadata from filename (e.g. company_027_italy_market_quantity_2025_q4.xlsx)
    file_name_lower = Path(file_name).name.lower()

    company = file_name_lower.split("_")[0].title()
    country = file_name_lower.split("_")[1].title()

    year_match = re.search(r"(20\d{2})", file_name_lower)
    year = int(year_match.group(1)) if year_match else None

    if "no_panel" in file_name_lower or "nopanel" in file_name_lower:
        is_panel = False
    elif "market" in file_name_lower:
        is_panel = True
    else:
        is_panel = None

    if "quantity" in file_name_lower:
        metric_type = "quantity"
    elif "sales" in file_name_lower or "value" in file_name_lower:
        metric_type = "gross_sales"
    else:
        raise ValueError(f"Cannot infer metric type from file name: {file_name}")

    df["year"]             = year
    df["company"]          = company
    df["country"]          = country
    df["is_panel"]         = is_panel
    df["source_file_name"] = Path(file_name).name

    month_cols = ["gen", "feb", "mar", "apr", "mag", "giu",
                  "lug", "ago", "set", "ott", "nov", "dic"]

    for m in month_cols:
        if m not in df.columns:
            df[m] = 0

    # Clean and convert each month column to numeric (inside the loop — all 12 months)
    for m in month_cols:
        df[m] = pd.to_numeric(
            df[m]
            .astype(str)
            .str.strip()
            .replace(["-", "", "nan", "None"], np.nan)
            .str.replace(",", ".", regex=False),
            errors="coerce"
        ).fillna(0)

    if "mese" in df.columns:
        df = df.drop(columns=["mese"])

    # Wide → long
    df_long = df.melt(
        id_vars=["famiglia", "articolo", "cic", "year", "company",
                 "country", "is_panel", "source_file_name"],
        value_vars=month_cols,
        var_name="month",
        value_name="value"
    )

    month_mapping = {
        "gen": 1, "feb": 2, "mar": 3, "apr": 4,
        "mag": 5, "giu": 6, "lug": 7, "ago": 8,
        "set": 9, "ott": 10, "nov": 11, "dic": 12
    }

    df_long["month_num"] = df_long["month"].map(month_mapping)
    df_long["date"] = pd.to_datetime({
        "year": df_long["year"],
        "month": df_long["month_num"],
        "day": 1
    }).dt.date

    # Drop zero-value rows (non-zero filter; NaN already filled to 0 above)
    df_long = df_long[df_long["value"] != 0]

    df_long["metric_type"] = metric_type

    df_long = df_long.rename(columns={
        "articolo":  "item_description",
        "famiglia":  "item_family",
        "cic":       "item_code",
    })

    df_long["item_categories"] = df_long["item_family"].apply(
        lambda x: [x] if pd.notna(x) else []
    )

    return df_long[[
        "date",
        "company",
        "country",
        "is_panel",
        "item_code",
        "item_description",
        "item_categories",
        "metric_type",
        "value",
        "source_file_name",
    ]].reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing logic — structure & decisions
# MAGIC
# MAGIC ### Source files
# MAGIC company_027 delivers **two separate Excel files per dataset**: one for quantities, one for sales values.
# MAGIC They share the same structure: rows = products, columns = months (Italian abbreviations: gen → dic).
# MAGIC Files are paired by dataset key derived from the filename (e.g. `company_027_italy_market_2025_q4`).
# MAGIC
# MAGIC ### File naming convention
# MAGIC `{company}_{country}_{panel_flag}_{metric}_{year}_{period}.xlsx`
# MAGIC
# MAGIC | Token | Values | Mapped to |
# MAGIC |---|---|---|
# MAGIC | `market` | panel data | `is_panel = True` |
# MAGIC | `nopanel` / `no_panel` | non-panel | `is_panel = False` |
# MAGIC | `quantity` | unit volumes | parsed as `quantity` |
# MAGIC | `value` | revenue | parsed as `gross_sales` |
# MAGIC
# MAGIC ### Item code (CIC) extraction
# MAGIC The `articolo` column contains both name and code in a single string: `"PRODUCT NAME (270432)"`.
# MAGIC The CIC is extracted from the trailing parentheses via regex and stored as `item_code`.
# MAGIC The parenthetical suffix is then removed from the description, and whitespace is normalized
# MAGIC (leading code tokens, double spaces, and tabs are collapsed).
# MAGIC
# MAGIC ### Month columns: numeric conversion
# MAGIC Each month column may contain European decimal notation (`","` as decimal separator) or
# MAGIC placeholder values (`"-"`, empty cells). The cleaning pipeline per column:
# MAGIC 1. Cast to string and strip whitespace
# MAGIC 2. Replace placeholder values (`"-"`, `""`, `"nan"`) with `NaN`
# MAGIC 3. Replace `","` with `"."` for decimal parsing
# MAGIC 4. Convert to float via `pd.to_numeric(errors="coerce")`, fill remaining `NaN` with `0`
# MAGIC
# MAGIC > **Note**: the `pd.to_numeric` call must be **inside** the loop — one conversion per month column.
# MAGIC > A prior version had it outside (only `dic` was converted); the other 11 months stayed as strings,
# MAGIC > causing mismatches in both `quantity` and `gross_sales` totals after the melt.
# MAGIC
# MAGIC ### Wide → long transform
# MAGIC After cleaning, the 12 month columns are melted into a single `(date, value)` pair per row.
# MAGIC Zero-value rows are dropped (items with no sales/quantity in that month).
# MAGIC
# MAGIC ### Quantity as float
# MAGIC company_027 quantities can be non-integer (e.g. fractional pack units).
# MAGIC `quantity` is stored as `DoubleType` — using `IntegerType` would silently truncate decimals.
# MAGIC
# MAGIC ### Merge strategy (quantity × value)
# MAGIC The two files are joined on `[date, item_code]` with an **outer join** to surface any mismatches:
# MAGIC - Items in quantity only → `gross_sales = 0`
# MAGIC - Items in value only → `quantity = 0`
# MAGIC - Both sides → merged normally
# MAGIC
# MAGIC The merge report prints key counts and any unmatched records to help detect upstream data gaps.

# COMMAND ----------

# ==========================================================
# BUILD BUSINESS KEY
# ==========================================================

def get_dataset_key(file_name: str) -> str:
    normalized_name = Path(file_name).name.lower()
    stem = re.sub(r"(?:\.xlsx?|\.xlsb)+$", "", normalized_name)
    return re.sub(r"_(quantity|value|sales)(?=_|$)", "", stem)


def combine_source_file_names(*file_names: str) -> str:
    valid_names = [
        str(file_name)
        for file_name in file_names
        if pd.notna(file_name) and str(file_name).strip()
    ]
    return " | ".join(dict.fromkeys(valid_names))


# ==========================================================
# PROCESS FILES
# ==========================================================

if df.count() == 0:

    print("No company_027 files found in latest batch.")

else:

    print(f"Files found: {df.count()}")

    rows = df.collect()
    grouped_files = defaultdict(dict)

    for row in rows:
        file_name    = row["file_name"]
        dataset_key  = get_dataset_key(file_name)
        if "quantity" in file_name.lower():
            grouped_files[dataset_key]["quantity"] = row
        elif "value" in file_name.lower() or "sales" in file_name.lower():
            grouped_files[dataset_key]["value"] = row

    all_results = []

    for dataset_key, files in grouped_files.items():

        print(f"Datasets found: {len(grouped_files)}")
        print(f"\nProcessing dataset: {dataset_key}")

        quantity_df = None
        value_df    = None

        if "quantity" not in files:
            warnings.warn(f"Missing quantity file for dataset {dataset_key}")
        if "value" not in files:
            warnings.warn(f"Missing value file for dataset {dataset_key}")

        if "quantity" in files:
            quantity_df = (
                parse_company_027_excel(
                    file_path=files["quantity"]["file_path"],
                    file_name=files["quantity"]["file_name"],
                )
                .drop(columns=["metric_type"])
                .rename(columns={"value": "quantity"})
            )

        if "value" in files:
            value_df = (
                parse_company_027_excel(
                    file_path=files["value"]["file_path"],
                    file_name=files["value"]["file_name"],
                )
                .drop(columns=["metric_type"])
                .rename(columns={"value": "gross_sales"})
            )

        # Remove all spaces from item_code before joining (codes must not have internal spaces)
        for part in [quantity_df, value_df]:
            if part is not None:
                part["item_code"] = part["item_code"].astype(str).str.replace(r"\s+", "", regex=True)

        join_cols = ["date", "item_code"]

        # Duplicate checks
        for label, part in [("QUANTITY", quantity_df), ("VALUE", value_df)]:
            if part is not None:
                dupes = part.duplicated(subset=join_cols, keep=False)
                if dupes.any():
                    print(f"\n=== DUPLICATE KEYS IN {label} ===")
                    display(part.loc[dupes].sort_values(join_cols))

        # Key coverage report
        if quantity_df is not None and value_df is not None:
            qty_keys = set(zip(quantity_df["date"], quantity_df["item_code"]))
            val_keys = set(zip(value_df["date"],    value_df["item_code"]))
            print(f"Quantity keys:       {len(qty_keys)}")
            print(f"Value keys:          {len(val_keys)}")
            print(f"Missing in quantity: {len(val_keys - qty_keys)}")
            print(f"Missing in value:    {len(qty_keys - val_keys)}")

        # Merge
        if quantity_df is not None and value_df is not None:

            merged = quantity_df.merge(
                value_df, on=join_cols, how="outer", indicator=True, suffixes=("_qty", "_val")
            )

            for side, label in [("right_only", "VALUE ONLY"), ("left_only", "QUANTITY ONLY")]:
                subset = merged[merged["_merge"] == side]
                if len(subset) > 0:
                    print(f"\n=== RECORDS PRESENT IN {label} ===")
                    display(subset[join_cols].sort_values(join_cols))

            merged["company"]          = merged["company_qty"].combine_first(merged["company_val"])
            merged["country"]          = merged["country_qty"].combine_first(merged["country_val"])
            merged["is_panel"]         = merged["is_panel_qty"].combine_first(merged["is_panel_val"])
            merged["item_description"] = merged["item_description_qty"].combine_first(merged["item_description_val"])
            merged["item_categories"]  = merged["item_categories_qty"].combine_first(merged["item_categories_val"])
            merged["quantity"]         = merged["quantity"].fillna(0)
            merged["gross_sales"]      = merged["gross_sales"].fillna(0)
            merged["file_name"] = merged.apply(
                lambda row: combine_source_file_names(
                    row.get("source_file_name_qty"),
                    row.get("source_file_name_val"),
                ),
                axis=1,
            )

        elif quantity_df is not None:
            merged = quantity_df.copy()
            merged["gross_sales"] = 0.0
            merged["file_name"] = merged["source_file_name"]

        elif value_df is not None:
            merged = value_df.copy()
            merged["quantity"] = 0.0
            merged["file_name"] = merged["source_file_name"]

        else:
            continue

        all_results.append(
            merged[[
                "date",
                "company",
                "country",
                "is_panel",
                "item_code",
                "item_description",
                "item_categories",
                "quantity",
                "gross_sales",
                "file_name",
            ]]
        )

    # Final dataset
    final_df = pd.concat(all_results, ignore_index=True)

    final_df["gross_sales"] = pd.to_numeric(final_df["gross_sales"], errors="coerce").fillna(0).round(2)
    final_df["quantity"]    = pd.to_numeric(final_df["quantity"],    errors="coerce").fillna(0).round(2)

    print(f"\nFinal dataset: {len(final_df)} rows")

# COMMAND ----------

final_df[final_df['item_code'] =='8014914'].sort_values(by="date")

# COMMAND ----------

schema = StructType([
    StructField("date", DateType(), True),
    StructField("source_row_id", IntegerType(), True),
    StructField("company_item_code", StringType(), True),
    StructField("company", StringType(), True),
    StructField("item_description", StringType(), True),
    StructField("product_family_level_1", StringType(), True),
    StructField("brand_company", StringType(), True),
    StructField("item_code", StringType(), True),
    StructField("channel_raw", StringType(), True),
    StructField("customer_raw", StringType(), True),
    StructField("destination_country_raw", StringType(), True),
    StructField("country_raw", StringType(), True),
    StructField("is_saliva_ejector", StringType(), True),
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

final_df["source_row_id"] = None
final_df["company_item_code"] = (
    final_df["company"].astype(str).str.upper()
    + "|"
    + final_df["item_code"].astype(str)
)
final_df["product_family_level_1"] = final_df["item_categories"].apply(
    lambda value: value[0] if isinstance(value, list) and len(value) > 0 else None
)
final_df["brand_company"] = "company_027"
final_df["channel_raw"] = "Wholesale"
final_df["customer_raw"] = final_df["is_panel"].map({True: "PANEL", False: "NO PANEL"})
final_df["destination_country_raw"] = final_df["country"]
final_df["country_raw"] = final_df["country"]
final_df["is_saliva_ejector"] = None
final_df["gross_sales_currency"] = "EUR"
final_df["time_frames"] = "month"
final_df["volumes"] = np.nan
final_df["data_matrix"] = None
final_df["data_collection"] = "sell-out"
final_df["project"] = "sell-out"

# Reorder columns to match schema positional order (Spark assigns types by position, not name)
schema_col_names = [f.name for f in schema.fields]
final_df = final_df[schema_col_names]


spark_df = spark.createDataFrame(final_df, schema=schema)

# Tag every row with the current batch for incremental downstream processing
spark_df = spark_df.withColumn("batch_id", F.lit(batch_id))

# Schema validation (ignore batch_id since it may not exist in target yet)
target_schema = spark.table(TARGET_INGESTION_TABLE).schema
output_signature = [(field.name, field.dataType.simpleString()) for field in spark_df.schema if field.name != "batch_id"]
target_signature = [(field.name, field.dataType.simpleString()) for field in target_schema if field.name != "batch_id"]

if output_signature != target_signature:
    raise ValueError(
        "company_027 output schema does not match landing_company_027. "
        f"Output: {output_signature}; target: {target_signature}"
    )


# =========================================
# DATA QUALITY CONTROL TOTALS
# =========================================

CONTROL_COLUMNS = ("quantity", "gross_sales")
CONTROL_DECIMAL_TYPE = "decimal(38,12)"


def control_totals(dataframe) -> dict:
    control_row = dataframe.agg(
        F.count("*").alias("row_count"),
        *[
            F.coalesce(
                F.sum(F.col(column_name).cast(CONTROL_DECIMAL_TYPE)),
                F.lit(0).cast(CONTROL_DECIMAL_TYPE),
            ).alias(column_name)
            for column_name in CONTROL_COLUMNS
        ],
    ).first()
    return {
        "row_count": control_row["row_count"],
        **{
            column_name: control_row[column_name]
            for column_name in CONTROL_COLUMNS
        },
    }


invalid_numeric_counts = spark_df.agg(
    *[
        F.sum(
            F.when(
                F.col(column_name).isNull() | F.isnan(F.col(column_name)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias(column_name)
        for column_name in CONTROL_COLUMNS
    ]
).first().asDict()

if any(invalid_numeric_counts.values()):
    raise ValueError(
        "Cannot append company_027 data: invalid numeric values found after "
        f"conversion: {invalid_numeric_counts}"
    )

excel_totals = control_totals(spark_df)
source_file_names = spark_df.select("file_name").distinct()


def landing_totals_for_source_files() -> dict:
    matching_landing_rows = spark.table(TARGET_INGESTION_TABLE).join(
        source_file_names,
        on="file_name",
        how="left_semi",
    )
    return control_totals(matching_landing_rows)


landing_totals_before = landing_totals_for_source_files()

print(f"Excel control totals: {excel_totals}")
print(f"Landing totals before append for the same source files: {landing_totals_before}")

# COMMAND ----------

spark_df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,for testing
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

landing_totals_after = landing_totals_for_source_files()
imported_totals = {
    metric_name: landing_totals_after[metric_name] - landing_totals_before[metric_name]
    for metric_name in ("row_count", *CONTROL_COLUMNS)
}

reconciliation_passed = imported_totals == excel_totals
reconciliation_outcome = "OK" if reconciliation_passed else "ERRORE"
reconciliation_table = pd.DataFrame(
    [
        {
            "fonte": "Excel",
            "quantity": excel_totals["quantity"],
            "gross_sales": excel_totals["gross_sales"],
            "esito": reconciliation_outcome,
        },
        {
            "fonte": "Import Table",
            "quantity": imported_totals["quantity"],
            "gross_sales": imported_totals["gross_sales"],
            "esito": reconciliation_outcome,
        },
    ]
)

display(reconciliation_table)

if not reconciliation_passed:
    raise ValueError(
        "company_027 landing reconciliation failed after append. "
        f"Expected from Excel: {excel_totals}; "
        f"observed landing increment: {imported_totals}. "
        "The append is already committed and requires manual remediation."
    )

print(f"✓ Landing reconciliation passed: {imported_totals}")

