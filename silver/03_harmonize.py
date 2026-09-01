# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pycountry==24.6.1",
# ]
# ///
# MAGIC %md
# MAGIC # Standardizzazione e Classificazione

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, ArrayType
from delta.tables import DeltaTable
from functools import reduce
import json
from itertools import islice
from pyspark.sql import DataFrame
from pyspark.sql.window import Window 

# COMMAND ----------

REF_COUNTRY_CURRENCY = (
    f"{CATALOG}.{SILVER_SCHEMA}.ref_country_currency"
)

REF_FX_NORMALIZED = (
    f"{CATALOG}.{SILVER_SCHEMA}.ref_fx_normalized"
)

SOURCE_DIM_PRODUCT_MASTER = (
    f"{CATALOG}.{SILVER_SCHEMA}.dim_product_master"
)

REF_CUSTOMERS_CHANNELS = (
    f"{CATALOG}.{SILVER_SCHEMA}.ref_customers_channels"
)

TARGET_HARMONIZED = (
    f"{CATALOG}.{SILVER_SCHEMA}.harmonized"
)

print(
    f"Source                       : "
    f"{CATALOG}.{BRONZE_SCHEMA}.landing_*"
)

print(
    f"Country currency ref         : "
    f"{REF_COUNTRY_CURRENCY}"
)

print(
    f"Normalized FX ref            : "
    f"{REF_FX_NORMALIZED}"
)

print(f"Product Master ref           : {SOURCE_DIM_PRODUCT_MASTER}")
print(f"Customer / Channel ref       : {REF_CUSTOMERS_CHANNELS}")

print(
    f"Target                       : "
    f"{TARGET_HARMONIZED}"
)


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
        .filter(F.col("batch_id").isNotNull())
        .orderBy(F.col("ingestion_time").desc())
        .select("batch_id")
        .first()
    )
    if latest_batch is None:
        raise ValueError(
            f"No batch found in {RAW_INGESTION_TABLE}"
        )
    batch_id = latest_batch["batch_id"]
    batch_id_source = "latest ingestion"

print(f"Batch ID ({batch_id_source}): {batch_id}")

# Persist sortable submission lineage into Silver so Gold can resolve resubmissions
# deterministically without relying on UUID ordering.
batch_meta = (
    spark.table(RAW_INGESTION_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .agg(F.max("ingestion_time").alias("submission_ts"))
    .first()
)
if batch_meta is None or batch_meta["submission_ts"] is None:
    raise ValueError(f"Cannot resolve ingestion timestamp for batch_id={batch_id}")

submission_ts = batch_meta["submission_ts"]
submission_id = int(submission_ts.strftime("%Y%m%d%H%M%S%f")[:17])
print(f"Submission ID: {submission_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Harmonize Sell-Out
# MAGIC
# MAGIC Legge le tabelle `bronze.landing_*` e scrive in `silver.harmonized` tramite Delta MERGE insert-only.
# MAGIC
# MAGIC Prerequisito: `02_ref_dimensions.ipynb` deve essere già eseguito — il notebook joina `silver.dim_companies`
# MAGIC per derivare `company_type` (manufacturer / dealer / company) dal nome azienda.
# MAGIC
# MAGIC Il mapping delle colonne sorgente per ogni tabella è definito in `field_mapping.json` (stesso repo).
# MAGIC Campi con variazioni tra tabelle: `company`, `customer_raw`, `destination_country_raw`, `channel_raw`, `volumes`, `sales`.
# MAGIC Anche `company_item_code` viene letto dalla colonna indicata in `field_mapping.json`.
# MAGIC Campi statici per tabella: `time_frame` (valore letterale, es. `"semi-annual"`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load field mapping & harmonized schema
# MAGIC
# MAGIC Imports the field mapping JSON (`FIELD_MAP`) that maps each landing table's columns to the harmonized model, and defines the target harmonized schema.

# COMMAND ----------

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir  = "/Workspace" + "/".join(_notebook_path.split("/")[:-1])

with open(f"{_notebook_dir}/field_mapping.json") as _f:
    FIELD_MAP = json.load(_f)

print(f"Loaded field_mapping.json — {len(FIELD_MAP)} tables")

# COMMAND ----------

# DBTITLE 1,Schema harmonize
def _col_or_lit(df, value):
    """
    Restituisce una colonna se value è presente nel DataFrame,
    un valore letterale se è una configurazione statica,
    oppure NULL se value è None.
    """
    if value is None:
        return F.lit(None)
    if value in df.columns:
        return F.col(value)
    return F.lit(value)


# Schema harmonized: (nome campo, tipo). L'ordine qui è l'ordine delle colonne in output.
HARMONIZED_SCHEMA = [
    ("date", "date"),
    ("company", "string"),
    ("company_type", "string"),
    # La colonna sorgente è definita in field_mapping.json.
    ("company_item_code", "string"),
    ("item_code", "string"),
    ("item_description", "string"),
    ("customer_raw", "string"),
    ("destination_country_raw", "string"),
    ("channel_raw", "string"),
    ("time_frames", "string"),
    ("volumes", "double"),
    ("quantity", "double"),
    ("sales", "double"),
    ("billing_currency", "string"),
    ("data_matrix", "string"),
    ("data_collection", "string"),
    ("project", "string"),
    ("file_name", "string"),
]


def map_to_harmonized(
    df: DataFrame,
    landing_key: str,
) -> DataFrame:
    """
    Mappa una tabella landing nello schema comune harmonized.

    Parameters
    ----------
    df:
        DataFrame della tabella landing.

    landing_key:
        Chiave della landing presente nel dizionario FIELD_MAP.

    Returns
    -------
    DataFrame
        DataFrame con lo schema harmonized comune.
    """

    if landing_key not in FIELD_MAP:
        raise ValueError(
            f"Mapping non trovato per la landing: {landing_key}"
        )

    m = FIELD_MAP[landing_key]

    mapped_columns = []

    for field, dtype in HARMONIZED_SCHEMA:
        source_value = m.get(field)

        mapped_columns.append(
            _col_or_lit(df, source_value).cast(dtype).alias(field)
        )

    mapped_df = df.select(*mapped_columns)

    # Canonical naming is applied before every Silver join.
    return mapped_df.withColumn(
        "company",
        normalize_company_name(F.col("company")),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harmonization
# MAGIC
# MAGIC Reads all `bronze.landing_*` tables, applies the column mapping from `field_mapping.json`, and unions into a single `harmonized_df`.
# MAGIC
# MAGIC - **All companies** (cell below): full historical dataset (~65M records) — use in production.
# MAGIC - **Testing scope**: restricted to 8 companies / H2 2025 — use during development.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full landing scan — all companies
# MAGIC
# MAGIC Scans all landing tables in bronze: **~65M records** across all companies.
# MAGIC Due to this volume, the harmonization step below runs on a **reduced testing scope** (8 companies, H2 2025).

# COMMAND ----------

# Audit: which landing tables exist in bronze vs which are missing
# found   = []
# missing = []

# for landing_key in FIELD_MAP:
#     table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{landing_key}"
#     if spark.catalog.tableExists(table_name):
#         found.append(landing_key)
#     else:
#         missing.append(landing_key)

# print(f"✓ Found   : {len(found):>3}  tables")
# print(f"✗ Missing : {len(missing):>3}  tables")

# if found:
#     print("\n--- FOUND ---")
#     for t in found:
#         print(f"  ✓  {t}")

# if missing:
#     print("\n--- MISSING ---")
#     for t in missing:
#         print(f"  ✗  {t}")

# COMMAND ----------

# DBTITLE 1,ALL companies
# ## ALL COMPANIES

# parts   = []
# skipped = []

# for landing_key in FIELD_MAP:
#     table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{landing_key}"

#     if not spark.catalog.tableExists(table_name):
#         skipped.append(landing_key)
#         continue

#     df     = spark.table(table_name)
#     mapped = map_to_harmonized(df, landing_key)
#     count  = mapped.count()
#     parts.append(mapped)
#     print(f"✓  {landing_key:40s} {count:>8,} rows")

# if skipped:
#     print(f"\n⚠️  Tabelle non trovate in bronze: {skipped}")

# harmonized_df = reduce(lambda a, b: a.unionByName(b), parts)
# print(f"\nTotal rows: {harmonized_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Testing scope filter
# MAGIC
# MAGIC **For testing purposes only**, the pipeline is restricted to reduce the record count:
# MAGIC - **Companies**: only the 8 in-scope companies — SellOut (company_027, company_039, company_040, company_045) and Flash (company_scope_001, company_scope_002, company_011, company_035)
# MAGIC - **Date range**: last 6 months of 2025 (2025-07-01 to 2025-12-31)
# MAGIC
# MAGIC > **TODO**: remove/extend these filters before running against the full dataset in production.

# COMMAND ----------

# DBTITLE 1,N companies
# SELLOUT_TABLES = [
#     "landing_company_040",
#     "landing_company_022",
#     "landing_company_027",
#     "landing_company_042",
#     "landing_company_045",
#     "landing_company_001",
#     "landing_company_030",
#     "landing_company_073",
# ]

# parts = []
# skipped = []

# START_DATE = "2025-01-01"
# END_DATE = "2025-12-31"

# # ============================================================
# # Lettura delle landing e filtro dati da armonizzare
# # La reference clienti/canali è già stata costruita con tutto
# # lo storico nel notebook 02_ref_customers_channels.
# # Qui filtriamo solamente i record da caricare in harmonized.
# # ============================================================

# for landing_key in SELLOUT_TABLES:

#     table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{landing_key}"

#     if not spark.catalog.tableExists(table_name):
#         skipped.append(landing_key)
#         continue

#     harm = (
#         map_to_harmonized(
#             spark.table(table_name),
#             landing_key,
#         )
#         .filter(
#             F.col("date").between(START_DATE, END_DATE)
#         )
#     )

#     parts.append(harm)
#     print(f"✓ {landing_key}")

# if skipped:
#     print(f"\n⚠️ Tables not found in bronze: {skipped}")

# harmonized_df = reduce(lambda a, b: a.unionByName(b), parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Ingestion

# COMMAND ----------

# DBTITLE 1,Incremental harmonization by batch_id
# =========================================
# INCREMENTAL HARMONIZATION — batch_id filter
# =========================================
# Only process landing tables that received data in the current
# batch (produced by 01_parsing_* tasks).

SELLOUT_TABLES = [
    "landing_company_040",
    "landing_company_027",
    "landing_company_045",
]

parts = []
skipped = []
empty = []

for landing_key in SELLOUT_TABLES:
    table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{landing_key}"

    if not spark.catalog.tableExists(table_name):
        skipped.append(landing_key)
        continue

    df = spark.table(table_name).filter(F.col("batch_id") == batch_id)

    # Skip tables with no records for this batch
    if df.limit(1).count() == 0:
        empty.append(landing_key)
        continue

    mapped = map_to_harmonized(df, landing_key)
    parts.append(mapped)
    print(f"✓  {landing_key}")

if skipped:
    print(f"\n⚠️  Tables not found in bronze: {skipped}")
if empty:
    print(f"\nⓘ  No data for batch {batch_id}: {empty}")

if not parts:
    raise ValueError(
        f"No landing tables contain data for batch_id={batch_id}"
    )

harmonized_df = reduce(lambda a, b: a.unionByName(b), parts)
print(
    f"\n✅ Harmonized {len(parts)} tables — "
    f"{harmonized_df.count():,} rows (batch: {batch_id})"
)

# COMMAND ----------

# Normalizza il paese di destinazione usando dictionary e fallback ISO.
harmonized_df = harmonized_df.withColumn(
    "destination_country_raw",
    F.trim(F.col("destination_country_raw")),
)

destination_country_lookup_df = (
    convert_to_country_iso(
        harmonized_df.select("destination_country_raw").dropDuplicates(),
        "destination_country_raw",
    )
    .select(
        F.col("country_raw").alias("destination_country_raw"),
        F.col("country").alias("destination_country"),
    )
)

harmonized_df = (
    harmonized_df
    .join(
        F.broadcast(destination_country_lookup_df),
        "destination_country_raw",
        "left",
    )
    .withColumn(
        "_missing_destination_country",
        F.col("destination_country").isNull()
        | (F.trim(F.col("destination_country")) == ""),
    )
)

# COMMAND ----------

harmonized_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrichment
# MAGIC
# MAGIC Enrich the Sell-Out data with all the business information required for the subsequent valorization phases:
# MAGIC
# MAGIC - Company type
# MAGIC - Product classification
# MAGIC - Channel standardization
# MAGIC - Currency conversion
# MAGIC - Data Quality checks
# MAGIC
# MAGIC Enriches `harmonized_df` through four sequential left joins to produce the final `enriched_df`.
# MAGIC
# MAGIC | Phase | Join source | Key | Fields added |
# MAGIC |---|---|---|---|
# MAGIC | **1 – Company type** | `silver.dim_companies` | `company` | `company_type` |
# MAGIC | **2 – Currency conversion** | `silver.ref_country_currency`  + `silver.ref_fx_normalized` | `year + month + destination_country` / `billing_currency + local_currency` | `local_currency`, `billing_local_fx`, `sales_local_currency` |
# MAGIC | **3 – Commercial classification events** | `Only algorythm` | `sales` + `quantity` | `sales_event_type`
# MAGIC | **4 – Product classification** | `silver.dim_product_master` | `company_item_code` | `brand`, `prefix_code`, `father_name`, `pack`, … `classification_status`, `is_new_description` |
# MAGIC | **5 – Customer / Channel enrichment** | `silver.ref_customers_channels` | `company + destination_country + customer_raw` | `customer`, `country`, `channel` |
# MAGIC
# MAGIC Data Quality checks for all phases are consolidated in the **Data Quality** section at the end of this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Company type
# MAGIC
# MAGIC Left join on `silver.dim_companies` using `company == name` (case-insensitive). Derives `company_type` (`manufacturer` / `dealer` / `company`). Falls back to the value from `field_mapping.json` if the company is not found in the dimension.

# COMMAND ----------

dim_company = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.dim_company"
    )
    .select(
        "name",
        "type",
    )
    .alias("c")
)

enriched_df = (
    harmonized_df.alias("h")

    .join(
        dim_company,

        F.upper(
            F.trim(
                F.col("h.company")
            )
        )
        ==
        F.upper(
            F.trim(
                F.col("c.name")
            )
        ),

        "left",
    )

    .withColumn(
        "company_type_enriched",

        F.coalesce(
            F.col("c.type"),
            F.col("h.company_type"),
        ),
    )

    .select(
        *[
            F.col(f"h.`{column_name}`")
            for column_name
            in harmonized_df.columns
            if column_name != "company_type"
        ],

        F.col(
            "company_type_enriched"
        ).alias("company_type"),
    )
)

print(
    f"Total rows after company enrichment: "
    f"{enriched_df.count():,}"
)

# COMMAND ----------

display(
    enriched_df
    .select("company", "company_type")
    .dropDuplicates()
    .orderBy("company")
    .limit(10)
)

missing_company_type_df = (
    enriched_df
    .filter(
        F.col("company_type").isNull()
        | (F.trim(F.col("company_type")) == "")
    )
    .groupBy("company")
    .count()
    .orderBy(F.desc("count"), "company")
)

if missing_company_type_df.limit(1).count() > 0:
    display(missing_company_type_df.limit(10))
    raise ValueError(
        "company_type non valorizzato per una o più company."
    )

print("company_type valorizzato per tutte le righe.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Currency conversion
# MAGIC
# MAGIC Converte il valore `sales` dalla valuta di fatturazione alla valuta locale del
# MAGIC Paese di destinazione, producendo `sales_local_currency`.
# MAGIC
# MAGIC Vengono utilizzate due reference:
# MAGIC
# MAGIC | Passaggio | Reference | Chiave | Campo aggiunto |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `ref_country_currency` | `year + month + destination_country` | `local_currency` |
# MAGIC | 2 | `ref_fx_normalized` | `year + period_value + time_frame='month' + destination_country + billing_currency + local_currency` | `billing_local_fx` |
# MAGIC
# MAGIC Nella nuova `ref_fx_normalized`, il mese è contenuto in `period_value` e il
# MAGIC tasso è contenuto in `exchange_rate`. Il campo `country_name` fa parte della
# MAGIC chiave perché consente di distinguere le serie associate alla valuta locale
# MAGIC dei diversi Paesi.
# MAGIC
# MAGIC Quando valuta di fatturazione e valuta locale coincidono, il tasso viene
# MAGIC forzato a `1.0`. Se una valuta o il tasso non sono disponibili,
# MAGIC `sales_local_currency` rimane `null`.
# MAGIC

# COMMAND ----------

# Validate reference tables
for table in [REF_CUSTOMERS_CHANNELS, REF_COUNTRY_CURRENCY, REF_FX_NORMALIZED]:
    if not spark.catalog.tableExists(table):
        raise ValueError(f"Reference not found: {table}")

# Country -> Local Currency reference
ref_country_currency = (
    spark.table(REF_COUNTRY_CURRENCY)
    .select(
        F.col("year").cast("int").alias("_cc_year"),
        F.col("month").cast("int").alias("_cc_month"),
        F.upper(F.trim(F.col("country_join_key"))).alias("_cc_country_key"),
        F.upper(F.trim(F.col("local_currency"))).alias("_cc_local_currency"),
    )
    .dropDuplicates(
        [
            "_cc_year",
            "_cc_month",
            "_cc_country_key",
        ]
    )
)

# FX reference
# La nuova reference contiene più granularità temporali.
# Per la join con la data della vendita utilizziamo solo il livello mensile.
ref_fx_normalized = (
    spark.table(REF_FX_NORMALIZED)
    .filter(
        F.lower(F.trim(F.col("time_frame")))
        == F.lit("month")
    )
    .select(
        F.col("year").cast("int").alias("_fx_year_ref"),
        F.col("period_value").cast("int").alias("_fx_month_ref"),
        F.upper(F.trim(F.col("country_name"))).alias(
            "_fx_country_name_key"
        ),
        F.upper(F.trim(F.col("currency_base"))).alias(
            "_fx_currency_base"
        ),
        F.upper(F.trim(F.col("currency_target"))).alias(
            "_fx_currency_target"
        ),
        F.col("exchange_rate").cast("double").alias(
            "_fx_billing_local_fx"
        ),
    )
    .dropDuplicates(
        [
            "_fx_year_ref",
            "_fx_month_ref",
            "_fx_country_name_key",
            "_fx_currency_base",
            "_fx_currency_target",
        ]
    )
)


# COMMAND ----------


# Prepare join keys
enriched_df = (
    enriched_df
    .withColumn("_fx_year", F.year(F.col("date")))
    .withColumn("_fx_month", F.month(F.col("date")))
    .withColumn("_country_join_key", F.upper(F.trim(F.col("destination_country"))))
    .withColumn("billing_currency", F.upper(F.trim(F.col("billing_currency"))))
)

# Country -> Local Currency
if "local_currency" in enriched_df.columns:
    enriched_df = enriched_df.drop("local_currency")

source_columns = enriched_df.columns


enriched_df = (
    enriched_df.alias("h")
    .join(
        F.broadcast(ref_country_currency).alias("cc"),
        (
            (F.col("h._fx_year") == F.col("cc._cc_year"))
            & (F.col("h._fx_month") == F.col("cc._cc_month"))
            & (F.col("h._country_join_key") == F.col("cc._cc_country_key"))
        ),
        "left",
    )
    .select(
        *[F.col(f"h.`{column}`") for column in source_columns],
        F.col("cc._cc_local_currency").alias("local_currency"),
    )
)

# Billing Currency -> Local Currency FX
if "billing_local_fx" in enriched_df.columns:
    enriched_df = enriched_df.drop("billing_local_fx")

source_columns = enriched_df.columns

enriched_df = (
    enriched_df.alias("h")
    .join(
        F.broadcast(ref_fx_normalized).alias("fx"),
        (
            (F.col("h._fx_year") == F.col("fx._fx_year_ref"))
            & (F.col("h._fx_month") == F.col("fx._fx_month_ref"))
            & (
                F.col("h._country_join_key")
                == F.col("fx._fx_country_name_key")
            )
            & (
                F.col("h.billing_currency")
                == F.col("fx._fx_currency_base")
            )
            & (
                F.col("h.local_currency")
                == F.col("fx._fx_currency_target")
            )
        ),
        "left",
    )
    .select(
        *[F.col(f"h.`{column}`") for column in source_columns],
        F.col("fx._fx_billing_local_fx").alias("billing_local_fx"),
    )
)

# FX rules
same_currency = (
    F.col("billing_currency") == F.col("local_currency")
)

missing_currency = (
    F.col("billing_currency").isNull()
    | F.col("local_currency").isNull()
)

missing_fx = (
    F.col("sales").isNull()
    | F.col("billing_local_fx").isNull()
)

enriched_df = (
    enriched_df
    .withColumn(
        "billing_local_fx",
        F.when(
            F.col("billing_currency").isNull() | F.col("local_currency").isNull(),
            F.lit(None).cast("double"),
        ).when(
            F.col("billing_currency") == F.col("local_currency"),
            F.lit(1.0),
        ).otherwise(F.col("billing_local_fx"))
    )
    .withColumn(
        "sales_local_currency",
        F.when(
            F.col("sales").isNull() | F.col("billing_local_fx").isNull(),
            F.lit(None).cast("double"),
        ).otherwise(F.col("sales") * F.col("billing_local_fx"))
    )
    .drop("_fx_year", "_fx_month", "_country_join_key")
)


# COMMAND ----------

display(
    enriched_df

    .select(
        "date",
        "company",
        "destination_country_raw",
        "sales",
        "billing_currency",
        "local_currency",
        "billing_local_fx",
        "sales_local_currency",
    )

    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commercial classification events
# MAGIC
# MAGIC I record Silver post-normalizzazione valutaria contengono coppie `(sales, quantity)` che riflettono eventi commerciali eterogenei: vendite ordinarie, resi fisici, sconti retroattivi, omaggi, servizi, rettifiche contabili. La classificazione `sales_event_type` etichetta ogni record sub-record con il proprio ruolo, derivato sintatticamente dai segni e dalle nullità di `sales` e `quantity` (convenzione: `0` e `NA` trattati come equivalenti — assenza di informazione).
# MAGIC
# MAGIC La classificazione opera **pre-consolidamento** per preservare la natura originale degli eventi. Il consolidamento a grain Gold usa `sales_event_type` come dimensione di raggruppamento — gli eventi diversi non si fondono mai cross-categoria.

# COMMAND ----------

# Classificazione degli eventi commerciali.
enriched_df = (
    enriched_df
    .withColumn(
        "_sales_int",
        F.when(F.col("sales") == 0, F.lit(None).cast("double"))
        .otherwise(F.col("sales")),
    )
    .withColumn(
        "_qty_int",
        F.coalesce(F.col("quantity"), F.col("volumes")),
    )
    .withColumn(
        "_qty_int",
        F.when(F.col("_qty_int") == 0, F.lit(None).cast("double"))
        .otherwise(F.col("_qty_int")),
    )
    .withColumn(
        "sales_event_type",
        F.when(
            F.col("_sales_int").isNull() & F.col("_qty_int").isNull(),
            "empty",
        )
        .when(
            (F.col("_sales_int") > 0) & (F.col("_qty_int") > 0),
            "commercial_sale",
        )
        .when(
            (F.col("_sales_int") < 0) & (F.col("_qty_int") < 0),
            "return",
        )
        .when(
            (F.col("_sales_int") < 0) & F.col("_qty_int").isNull(),
            "rebate",
        )
        .when(
            F.col("_sales_int").isNull() & (F.col("_qty_int") > 0),
            "freebie",
        )
        .when(
            (F.col("_sales_int") > 0) & F.col("_qty_int").isNull(),
            "service",
        )
        .when(
            ((F.col("_sales_int") > 0) & (F.col("_qty_int") < 0))
            | ((F.col("_sales_int") < 0) & (F.col("_qty_int") > 0)),
            "accounting_adjustment",
        )
        .otherwise("anomaly"),
    )
    .drop("_sales_int", "_qty_int")
)

sales_event_type_levels = [
    "commercial_sale",
    "return",
    "rebate",
    "freebie",
    "service",
    "accounting_adjustment",
    "empty",
    "anomaly",
]

invalid_sales_event_type_df = enriched_df.filter(
    F.col("sales_event_type").isNull()
    | ~F.col("sales_event_type").isin(sales_event_type_levels)
)

if invalid_sales_event_type_df.limit(1).count() > 0:
    display(
        invalid_sales_event_type_df
        .groupBy("sales_event_type")
        .count()
    )
    raise ValueError("sales_event_type nullo o fuori dall'enum previsto.")

sales_event_order = F.create_map(
    *[
        item
        for position, event_type in enumerate(sales_event_type_levels)
        for item in (F.lit(event_type), F.lit(position))
    ]
)

sales_event_distribution_df = (
    enriched_df
    .groupBy("sales_event_type")
    .count()
    .withColumnRenamed("count", "n")
    .withColumn(
        "pct",
        F.round(
            F.col("n") / F.sum("n").over(Window.partitionBy()) * 100,
            2,
        ),
    )
    .withColumn("_sort_order", sales_event_order[F.col("sales_event_type")])
    .orderBy("_sort_order")
    .drop("_sort_order")
)

display(sales_event_distribution_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Classification
# MAGIC
# MAGIC Each sell-out record is matched against the Product Master (`dim_product_master`) using `company_item_code`.
# MAGIC
# MAGIC Three cases are handled:
# MAGIC
# MAGIC | Case | Condition | Action |
# MAGIC |------|-----------|--------|
# MAGIC | **1 – Known product** | Match found, description matches | Classification attributes attached as-is |
# MAGIC | **2 – Description mismatch** | Match found, description differs | Classification reused; `is_new_description = True` for manual review |
# MAGIC | **3 – New product** | No match | Record written to `silver.product_to_classify` with status `New`, pending manual classification |
# MAGIC
# MAGIC **Attributes retrieved from `dim_product_master`:** brand, prefix_code, father_name, pack, inner_count, inner_qty, pack_measure_unit, measure, feature, extra, mc_code, sellout_dm *(TODO)*, flash_implant / flash_equipment / flash_ortho *(TODO)*.

# COMMAND ----------

dim_pm = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_product_master")
    .select(
        F.concat_ws(
            "|",
            F.trim(F.col("company")),
            F.trim(F.col("item_code")),
        ).alias("_pm_company_item_code"),
        F.col("item_code").alias("_pm_item_code"),
        F.col("description").alias("_pm_description"),  # used for description mismatch detection only
        F.col("brand"),              # Sell-Out Brand
        F.col("prefix_code"),        # Prefix Code
        F.col("father_name"),        # Father Name
        F.col("pack"),               # Packaging
        F.col("inner_count"),        # Content
        F.col("inner_qty"),          # UM Packaging inner qty
        F.col("pack_measure_unit"),  # Unit of Measure
        F.col("measure"),            # Measure
        F.col("feature"),            # Type / Feature
        F.col("extra"),              # Extra
        F.col("mc_code"),
        # F.lit(None).cast("double").alias("sellout_dm"),       # Sell-Out DM — TODO
        # F.lit(None).cast("boolean").alias("flash_implant"),   # Flash Implant — TODO
        # F.lit(None).cast("boolean").alias("flash_equipment"), # Flash Equipment — TODO
        # F.lit(None).cast("boolean").alias("flash_ortho"),     # Flash Ortho — TODO
    )
)

_desc_mismatch = (
    F.col("_pm_item_code").isNotNull() &
    (F.lower(F.trim(F.col("item_description"))) != F.lower(F.trim(F.col("_pm_description"))))
)

enriched_df = (
    enriched_df.alias("e")
    .join(
        dim_pm.alias("pm"),
        F.upper(F.trim(F.col("e.company_item_code")))
        == F.upper(F.trim(F.col("pm._pm_company_item_code"))),
        "left",
    )
    # classification_status: "New" | "Description Changed" | "Matched"
    .withColumn(
        "classification_status",
        F.when(F.col("_pm_item_code").isNull(), F.lit("New"))
         .when(_desc_mismatch, F.lit("Description Changed"))
         .otherwise(F.lit("Matched")),
    )
    # is_new_description: True when the product exists in PDB but the incoming description differs
    .withColumn(
        "is_new_description",
        F.when(_desc_mismatch, F.lit(True)).otherwise(F.lit(False)),
    )
    .drop("_pm_description")
    # Le chiavi tecniche PM sono mantenute fino alla cella successiva.
)

print(f"Rows after product join: {enriched_df.count():,}")
enriched_df.groupBy("classification_status").count().orderBy("classification_status").show()

# COMMAND ----------

TARGET_PRODUCT_TO_CLASSIFY = f"{CATALOG}.{SILVER_SCHEMA}.product_to_classify"

# Case 3 – New Products: no match on company + item_code
# Deduplicated by (company, item_code): first description seen, earliest date, first source file.
new_products = (
    enriched_df
    .filter(F.col("classification_status") == "New")
    .groupBy("company", "item_code")
    .agg(
        F.first("item_description").alias("description"),
        F.min("date").alias("first_received_date"),
        F.first("file_name").alias("source_file"),
    )
    .withColumn("search_type", F.lit("sell-out"))
    .withColumn("status", F.lit("New"))
    .withColumn("created_date", F.current_date())
)

# MERGE insert-only: add only (company, item_code) pairs not yet tracked,
# preserving status updates made manually in the monitoring table.

if spark.catalog.tableExists(TARGET_PRODUCT_TO_CLASSIFY):
    (
        DeltaTable.forName(spark, TARGET_PRODUCT_TO_CLASSIFY)
        .alias("t")
        .merge(
            new_products.alias("s"),
            "t.company = s.company AND t.item_code = s.item_code",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        new_products.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_PRODUCT_TO_CLASSIFY)
    )

n_new = new_products.count()
print(f"New products pending classification : {n_new:,}  → {TARGET_PRODUCT_TO_CLASSIFY}")

# Drop join keys — no longer needed downstream
enriched_df = enriched_df.drop("_pm_item_code", "_pm_company_item_code")

# COMMAND ----------

enriched_df.groupBy('company',  "classification_status").count().orderBy('count', ascending=False).display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer / Channel enrichment
# MAGIC
# MAGIC Questa sezione arricchisce `enriched_df` (già arricchito con company_type, currency
# MAGIC conversion e product classification) utilizzando la reference Silver
# MAGIC `ref_customers_channels`, prodotta dal notebook `02_ref_customers_channels`.
# MAGIC
# MAGIC La logica applicata è la seguente:
# MAGIC
# MAGIC Il notebook 02 gestisce già country mapping, controlli, conflitti e deduplicazione.
# MAGIC Qui vengono soltanto preparate le chiavi del dato in ingresso ed eseguita una
# MAGIC `left join` con la reference, mantenendo tutte le righe di `enriched_df`.
# MAGIC
# MAGIC I controlli di Data Quality su questa join (row count, mapping rate, dettaglio unmatched, company check) sono in fondo al notebook, nella sezione **Data Quality**.
# MAGIC
# MAGIC Tutte le landing, inclusa `landing_company_001`, vengono gestite con la stessa logica di join.
# MAGIC

# COMMAND ----------

# La funzione lavora prima sulle chiavi univoche
# company + destination_country + customer_raw, registra quelle mancanti
# nella reference e restituisce customer, country e channel.
customer_channel_rows_before = enriched_df.count()

enriched_df = enrich_with_ref_customers_channels(
    enriched_df,
    REF_CUSTOMERS_CHANNELS,
)

customer_channel_rows_after = enriched_df.count()
if customer_channel_rows_before != customer_channel_rows_after:
    raise ValueError(
        "La join con ref_customers_channels ha modificato il numero di righe."
    )

print("Customer / Channel enrichment completato tramite reference Silver")
display(enriched_df.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality
# MAGIC
# MAGIC Post-enrichment checks across all four enrichment phases. Run these after `enriched_df` is fully built to validate coverage and integrity before writing to `silver.harmonized`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Currency Conversion
# MAGIC
# MAGIC Checks coverage of the FX join: countries without a `local_currency` mapping, company/month/currency combinations without an FX rate, and overall `sales_local_currency` coverage across rows with sales.

# COMMAND ----------

missing_local_currency = (
    enriched_df
    .filter(F.col("destination_country_raw").isNotNull() & F.col("local_currency").isNull())
    .groupBy("destination_country_raw")
    .count()
    .orderBy(F.desc("count"))
)
print(f"Countries without local_currency mapping: {missing_local_currency.count():,}")
display(missing_local_currency.limit(10))

missing_fx = (
    enriched_df
    .filter(
        F.col("sales").isNotNull()
        & F.col("billing_currency").isNotNull()
        & F.col("local_currency").isNotNull()
        & F.col("billing_local_fx").isNull()
    )
    .groupBy(
        "company",
        F.year(F.col("date")).alias("year"),
        F.month(F.col("date")).alias("month"),
        "billing_currency",
        "local_currency",
    )
    .count()
    .orderBy(F.desc("count"))
)
display(missing_fx.limit(10))

fx_coverage = (
    enriched_df
    .filter(F.col("sales").isNotNull())
    .agg(
        F.count("*").alias("rows_with_sales"),
        F.sum(F.when(F.col("sales_local_currency").isNotNull(), 1).otherwise(0)).alias("rows_converted"),
    )
    .withColumn("rows_not_converted", F.col("rows_with_sales") - F.col("rows_converted"))
    .withColumn("coverage_pct", F.round(F.col("rows_converted") / F.col("rows_with_sales") * 100, 2))
)
display(fx_coverage.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Classification
# MAGIC
# MAGIC Distribution of `classification_status` (Matched / Description Changed / New) by company, and null-rate check on the main product attributes.

# COMMAND ----------

enriched_df.groupBy("company", "classification_status").count().orderBy("count", ascending=False).limit(10).display()

total = enriched_df.count()
print(f"Rows after product classification : {total:,}")
check_cols = ["brand", "prefix_code", "father_name", "pack", "inner_count", "pack_measure_unit", "mc_code"]
for c in check_cols:
    nulls = enriched_df.filter(F.col(c).isNull()).count()
    print(f"  {c:25s}  null: {nulls:>8,} / {total:,}  ({100*nulls/total:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer / Channel Reference
# MAGIC
# MAGIC Checks coverage of the `ref_customers_channels` join: row-count preservation, mapping status breakdown, unmatched keys, company coverage and overall match rate.

# COMMAND ----------

print(f"Righe prima della join: {customer_channel_rows_before:,}")
print(f"Righe dopo la join:     {customer_channel_rows_after:,}")
print("Numero di righe preservato")

customer_channel_summary_df = (
    enriched_df
    .groupBy("company")
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(F.col("customer").isNull().cast("int")).alias("missing_customer"),
        F.sum(F.col("destination_country").isNull().cast("int")).alias("missing_destination_country"),
        F.sum(F.col("channel").isNull().cast("int")).alias("missing_channel"),
    )
    .orderBy("company")
)
display(customer_channel_summary_df.limit(10))

customer_channel_unmatched_df = (
    enriched_df
    .filter(
        F.col("customer").isNull()
        & F.col("destination_country").isNull()
        & F.col("channel").isNull()
    )
    .groupBy(
        "company",
        "customer_raw",
        "channel_raw",
    )
    .count()
    .orderBy("company", F.col("count").desc())
)
display(customer_channel_unmatched_df.limit(10))

# Riepilogo finale da usare nella call di validazione del layer armonizzato.
validation_summary_df = (
    enriched_df
    .groupBy("company")
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                F.col("customer").isNotNull()
                | F.col("destination_country").isNotNull()
                | F.col("channel").isNotNull(),
                1,
            ).otherwise(0)
        ).alias("utility_matched_rows"),
        F.sum(
            F.when(
                F.col("customer").isNull()
                & F.col("destination_country").isNull()
                & F.col("channel").isNull(),
                1,
            ).otherwise(0)
        ).alias("utility_unmatched_rows"),
        F.sum(
            F.when(F.col("classification_status") == "Matched", 1).otherwise(0)
        ).alias("products_matched"),
        F.sum(
            F.when(F.col("classification_status") == "New", 1).otherwise(0)
        ).alias("new_products"),
        F.sum(
            F.when(
                F.col("sales").isNotNull()
                & F.col("sales_local_currency").isNull(),
                1,
            ).otherwise(0)
        ).alias("rows_without_fx"),
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date"),
        F.sum("sales").alias("total_sales"),
        F.sum("quantity").alias("total_quantity"),
    )
    .withColumn(
        "utility_match_pct",
        F.round(
            F.col("utility_matched_rows") / F.col("total_rows") * 100,
            2,
        ),
    )
    .orderBy("company")
)

display(validation_summary_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Row Count Validation
# MAGIC
# MAGIC Verifies that all enrichment joins are row-preserving (left joins only — no records should be added or lost).

# COMMAND ----------

harmonized_count = harmonized_df.count()
enriched_count = enriched_df.count()
print(f"Harmonized rows: {harmonized_count:,}")
print(f"Enriched rows  : {enriched_count:,}")
if harmonized_count != enriched_count:
    raise ValueError("The enrichment joins changed the number of Sell-Out rows.")
print("✓ Row count preserved.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ENRICHED DATA FRAME

# COMMAND ----------

enriched_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Collection
# MAGIC
# MAGIC Checks completeness of data for the current batch. The semester is auto-detected from today's date — no manual configuration required. Only set `EXPECTED_COMPANIES` with the list of companies expected in this batch before running.

# COMMAND ----------

# ============================================================
# DATA QUALITY - COMPLETEZZA DATI PER AZIENDA
# Periodo controllato: secondo semestre 2025
# ============================================================

# BATCH_YEAR = 2025
# BATCH_SEMESTER = "H2"

# SEMESTER_MONTHS = {
#     "H1": [1, 2, 3, 4, 5, 6],
#     "H2": [7, 8, 9, 10, 11, 12],
# }

# # Elenco opzionale delle aziende attese.
# # Lasciare [] per non eseguire il controllo di completezza.
# EXPECTED_COMPANIES = []

# batch_months = SEMESTER_MONTHS[BATCH_SEMESTER]

# print(
#     f"Periodo Data Quality: "
#     f"{BATCH_YEAR}_{BATCH_SEMESTER}"
# )

# # enriched_df contiene già solamente i dati elaborati dall'harmonize.
# # Applichiamo comunque il filtro sul semestre per rendere il controllo
# # esplicito e indipendente dalle celle precedenti.
# dq_df = (
#     enriched_df
#     .filter(
#         (F.year(F.col("date")) == BATCH_YEAR)
#         & F.month(F.col("date")).isin(batch_months)
#     )
# )

# # Riepilogo dei dati disponibili per ciascuna azienda
# collection_summary_df = (
#     dq_df
#     .groupBy("company")
#     .agg(
#         F.count("*").alias("records"),
#         F.min("date").alias("first_date"),
#         F.max("date").alias("last_date"),
#     )
#     .orderBy("company")
# )

# # Aziende effettivamente presenti nel periodo
# companies_present = [
#     row["company"]
#     for row in (
#         dq_df
#         .select("company")
#         .where(F.col("company").isNotNull())
#         .distinct()
#         .collect()
#     )
# ]

# companies_present = sorted(companies_present)

# print(
#     f"✓ Aziende con dati in "
#     f"{BATCH_YEAR}_{BATCH_SEMESTER}: "
#     f"{len(companies_present)}"
# )

# print(
#     f"✓ Record complessivi analizzati: "
#     f"{dq_df.count():,}"
# )

# display(collection_summary_df)

# # Controllo opzionale rispetto alle aziende attese
# if EXPECTED_COMPANIES:

#     expected_companies_normalized = sorted(set(EXPECTED_COMPANIES))

#     missing_companies = [
#         company
#         for company in expected_companies_normalized
#         if company not in companies_present
#     ]

#     unexpected_companies = [
#         company
#         for company in companies_present
#         if company not in expected_companies_normalized
#     ]

#     if missing_companies:
#         print(
#             "\n⚠️ Aziende attese ma senza dati: "
#             f"{missing_companies}"
#         )

#     if unexpected_companies:
#         print(
#             "\n⚠️ Aziende presenti ma non previste: "
#             f"{unexpected_companies}"
#         )

#     if not missing_companies and not unexpected_companies:
#         print(
#             f"\n✓ Tutte le {len(expected_companies_normalized)} "
#             "aziende attese sono presenti."
#         )

# else:
#     print(
#         "\nℹ️ Controllo aziende attese non eseguito: "
#         "EXPECTED_COMPANIES è vuoto."
#     )

# COMMAND ----------

# DBTITLE 1,Write harmonized data (append)
# ============================================================
# WRITE TO SILVER — APPEND
# ============================================================

TARGET_HARMONIZED_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.harmonized"

# Final schema selection & rename
# Add lineage columns after all business transformations.
enriched_df = (
    enriched_df
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("submission_ts", F.lit(submission_ts).cast("timestamp"))
    .withColumn("submission_id", F.lit(submission_id).cast("long"))
)

enriched_df = enriched_df.select(
    "company",
    "company_type",
    "date",
    "batch_id",
    "submission_ts",
    "submission_id",
    "time_frames",
    "destination_country_raw",
    "destination_country",
    "_missing_destination_country",
    "customer_raw",
    "customer",
    "country",
    "channel_raw",
    "channel",
    "item_code",
    "company_item_code",
    "item_description",
    F.col("is_new_description"),
    F.col("mc_code").alias("master_code"),
    "brand",
    "prefix_code",
    "father_name",
    "pack",
    "inner_count",
    "inner_qty",
    "pack_measure_unit",
    "measure",
    "feature",
    "extra",
    "classification_status",
    "local_currency",
    "sales_local_currency",
    "billing_currency",
    "billing_local_fx",
    "sales_event_type",
    "volumes",
    "quantity",
    "sales",
    "data_matrix",
    "data_collection",
    "project",
    "file_name",
)

(
    enriched_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(TARGET_HARMONIZED_TABLE)
)

print(f"✓ Appended {enriched_df.count():,} rows to {TARGET_HARMONIZED_TABLE}")