# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pycountry==24.6.1",
# ]
# ///
# MAGIC %md
# MAGIC # Reference Customer / Channel
# MAGIC
# MAGIC Ricostruisce `silver.ref_customers_channels` dalle tabelle `bronze.landing_*` censite in
# MAGIC `field_mapping.json`.
# MAGIC
# MAGIC La tabella PDB `utility_customers_channels` non viene usata: le landing
# MAGIC legacy sono la sorgente dei valori gia codificati. Il PDB viene usato solo
# MAGIC come lookup per normalizzare country e channel.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dipendenza `pycountry`
# MAGIC
# MAGIC Usata dal fallback di `convert_to_country_iso()` per replicare il tentativo
# MAGIC automatico di standardizzazione country.

# COMMAND ----------

# MAGIC %pip install pycountry==24.6.1

# COMMAND ----------

# MAGIC %restart_python 

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

from functools import reduce
import json

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# ============================================================
# 1. CONFIGURAZIONE
# ============================================================

SOURCE_CHANNELS = f"{REFERENCE_CATALOG}.{UTILS_SCHEMA}.channels"

TARGET_REF_CC = f"{CATALOG}.{SILVER_SCHEMA}.ref_customers_channels"

# Lista temporanea per limitare le landing da elaborare.
# Esempi:
#   SELECTED_LANDING_COMPANIES = []
#   SELECTED_LANDING_COMPANIES = ["company_001", "company_027"]
# Se vuota, vengono elaborate tutte le landing censite in field_mapping.json.
SELECTED_LANDING_COMPANIES = []

_notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
_notebook_dir = "/Workspace" + "/".join(_notebook_path.split("/")[:-1])

with open(f"{_notebook_dir}/field_mapping.json") as _f:
    FIELD_MAP = json.load(_f)

print(f"Landing mapping       : {_notebook_dir}/field_mapping.json")
print(f"Landing censite       : {len(FIELD_MAP)}")
print(f"Channels              : {SOURCE_CHANNELS}")
print(f"Target                : {TARGET_REF_CC}")
print(f"Landing filter        : {SELECTED_LANDING_COMPANIES or 'ALL'}")

# COMMAND ----------

# ============================================================
# 2. RESET TARGET
# ============================================================

if spark.catalog.tableExists(TARGET_REF_CC):
    spark.sql(f"TRUNCATE TABLE {TARGET_REF_CC}")
    print(f"Tabella svuotata      : {TARGET_REF_CC}")
else:
    print(f"Tabella non presente  : {TARGET_REF_CC}")

# COMMAND ----------

# ============================================================
# 3. FUNZIONI DI SUPPORTO CUSTOMER / CHANNEL
# ============================================================


def _pick_existing_column(columns, candidates):
    columns_by_lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate and candidate.lower() in columns_by_lower:
            return columns_by_lower[candidate.lower()]
    return None


def _column_or_null(df, mapping, mapping_key, candidates=None):
    candidates = candidates or []
    mapped_name = mapping.get(mapping_key)

    column_name = _pick_existing_column(
        df.columns,
        [mapped_name, *candidates],
    )

    if column_name is None:
        return F.lit(None).cast("string")

    return F.col(f"`{column_name}`").cast("string")


def _company_from_landing_key(landing_key):
    return landing_key.replace("landing_", "").upper()


def _landing_company_key(value):
    return (
        str(value)
        .strip()
        .lower()
        .replace("landing_", "")
        .replace("-", "_")
        .replace(" ", "_")
    )


def _landing_is_selected(landing_key):
    selected = [_landing_company_key(v) for v in SELECTED_LANDING_COMPANIES if str(v).strip()]
    return not selected or _landing_company_key(landing_key) in selected


def _prepare_channels_reference():
    if not spark.catalog.tableExists(SOURCE_CHANNELS):
        raise ValueError(f"Channels table non trovata: {SOURCE_CHANNELS}")

    channels_df = spark.table(SOURCE_CHANNELS)
    columns = channels_df.columns

    id_col = _pick_existing_column(
        columns,
        ["id", "id_channel", "channel_id", "idchannel"],
    )
    name_col = _pick_existing_column(
        columns,
        ["channel", "name", "channel_name", "description", "label"],
    )

    if id_col is None or name_col is None:
        raise ValueError(
            f"Impossibile identificare colonne id/nome in {SOURCE_CHANNELS}. "
            f"Colonne disponibili: {columns}"
        )

    return (
        channels_df
        .select(
            F.col(f"`{id_col}`").cast("string").alias("_channel_id_key"),
            F.col(f"`{id_col}`").cast("long").alias("id_channel"),
            F.upper(F.trim(F.col(f"`{name_col}`").cast("string"))).alias(
                "_channel_name_key"
            ),
            F.trim(F.col(f"`{name_col}`").cast("string")).alias("channel"),
        )
        .filter(F.col("channel").isNotNull() & (F.col("channel") != ""))
        .dropDuplicates(["_channel_id_key", "_channel_name_key", "id_channel", "channel"])
    )


# COMMAND ----------

# ============================================================
# 4. RACCOLTA UNIVOCI DALLE LANDING
# ============================================================

landing_parts = []
skipped_landing_tables = []

for landing_key, mapping in FIELD_MAP.items():
    if not _landing_is_selected(landing_key):
        continue

    table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{landing_key}"

    if not spark.catalog.tableExists(table_name):
        skipped_landing_tables.append(landing_key)
        continue

    source_df = spark.table(table_name)

    company_expr = _column_or_null(
        source_df,
        mapping,
        "company",
        ["company", "dealer", "warehouse_or_depot", "brand_company"],
    )
    customer_raw_expr = _column_or_null(
        source_df,
        mapping,
        "customer_raw",
        ["customer_raw", "customer", "wholesale_customer", "customer_group"],
    )
    clean_customer_col = _pick_existing_column(
        source_df.columns,
        [
            mapping.get("clean_customer"),
            "clean_customer",
            "customer_clean",
            "customer_cleaned",
            "customer_pulito",
        ],
    )
    customer_expr = (
        F.col(f"`{clean_customer_col}`").cast("string")
        if clean_customer_col is not None
        else customer_raw_expr
    )
    country_raw_expr = _column_or_null(
        source_df,
        mapping,
        "country_raw",
        ["country", "country_raw"],
    )
    destination_country_raw_expr = _column_or_null(
        source_df,
        mapping,
        "destination_country_raw",
        [
            "destination_country_raw",
            "sales_country",
            "country_sales",
            "destination_country",
        ],
    )
    channel_raw_expr = _column_or_null(
        source_df,
        mapping,
        "channel_raw",
        ["channel_raw"],
    )

    landing_ref_df = (
        source_df
        .select(
            company_expr.alias("company"),
            destination_country_raw_expr.alias("destination_country_raw"),
            customer_raw_expr.alias("customer_raw"),
            customer_expr.alias("customer"),
            country_raw_expr.alias("country_raw"),
            channel_raw_expr.alias("channel_raw"),
        )
        .withColumn(
            "company",
            F.coalesce(
                normalize_company_name(F.col("company")),
                F.lit(_company_from_landing_key(landing_key)),
            ),
        )
        .withColumn("destination_country_raw", F.trim(F.col("destination_country_raw")))
        .withColumn("customer_raw", F.trim(F.col("customer_raw")))
        .withColumn("customer", F.coalesce(F.trim(F.col("customer")), F.col("customer_raw")))
        .withColumn("country_raw", F.trim(F.col("country_raw")))
        .withColumn("channel_raw", F.trim(F.col("channel_raw")))
        .filter(F.col("company").isNotNull() & (F.col("company") != ""))
        .filter(
            F.col("destination_country_raw").isNotNull()
            | F.col("customer_raw").isNotNull()
            | F.col("country_raw").isNotNull()
            | F.col("channel_raw").isNotNull()
        )
        .dropDuplicates(
            [
                "company",
                "destination_country_raw",
                "customer_raw",
                "customer",
                "country_raw",
                "channel_raw",
            ]
        )
    )

    landing_parts.append(landing_ref_df)

if not landing_parts:
    raise ValueError("Nessuna landing disponibile tra quelle censite nel field_mapping.json.")

raw_ref_df = reduce(lambda a, b: a.unionByName(b), landing_parts)

print(f"Landing lette             : {len(landing_parts)}")
print(f"Landing non trovate       : {len(skipped_landing_tables)}")
print(f"Righe raw univoche        : {raw_ref_df.count():,}")

if skipped_landing_tables:
    print(f"Prime landing non trovate : {skipped_landing_tables[:20]}")

# COMMAND ----------

# ============================================================
# 5. DEDUPLICA AL GRAIN RICHIESTO
# ============================================================

grain_cols = ["company", "destination_country_raw", "customer_raw"]

conflicting_attributes_df = (
    raw_ref_df
    .groupBy(*grain_cols)
    .agg(
        F.countDistinct("customer").alias("n_customer_values"),
        F.countDistinct("country_raw").alias("n_country_raw_values"),
        F.countDistinct("channel_raw").alias("n_channel_raw_values"),
    )
    .filter(
        (F.col("n_customer_values") > 1)
        | (F.col("n_country_raw_values") > 1)
        | (F.col("n_channel_raw_values") > 1)
    )
)

if conflicting_attributes_df.limit(1).count() > 0:
    print("Conflitti sul grain company / destination_country_raw / customer_raw:")
    display(conflicting_attributes_df.orderBy(F.desc("n_channel_raw_values")).limit(100))

base_ref_df = (
    raw_ref_df
    .groupBy(*grain_cols)
    .agg(
        F.first("customer", ignorenulls=True).alias("customer"),
        F.first("country_raw", ignorenulls=True).alias("country_raw"),
        F.first("channel_raw", ignorenulls=True).alias("channel_raw"),
    )
)

print(f"Righe dopo deduplica      : {base_ref_df.count():,}")

# COMMAND ----------

# ============================================================
# 6. COUNTRY STANDARDIZATION
# ============================================================

country_values_df = (
    base_ref_df
    .select(F.col("destination_country_raw").alias("country_to_convert"))
    .unionByName(base_ref_df.select(F.col("country_raw").alias("country_to_convert")))
)

country_lookup_df = convert_to_country_iso(country_values_df, "country_to_convert")

destination_country_lookup_df = (
    country_lookup_df
    .select(
        F.col("country_raw").alias("_destination_country_raw"),
        F.col("id_country").alias("id_destination_country"),
        F.col("country").alias("destination_country"),
    )
)

customer_country_lookup_df = (
    country_lookup_df
    .select(
        F.col("country_raw").alias("_country_raw"),
        F.col("id_country").alias("id_country"),
        F.col("country").alias("country"),
    )
)

ref_with_country_df = (
    base_ref_df.alias("r")
    .join(
        F.broadcast(destination_country_lookup_df).alias("dc"),
        F.upper(F.trim(F.col("r.destination_country_raw")))
        == F.upper(F.trim(F.col("dc._destination_country_raw"))),
        "left",
    )
    .join(
        F.broadcast(customer_country_lookup_df).alias("cc"),
        F.upper(F.trim(F.col("r.country_raw")))
        == F.upper(F.trim(F.col("cc._country_raw"))),
        "left",
    )
    .select(
        "r.company",
        "r.destination_country_raw",
        "dc.id_destination_country",
        "dc.destination_country",
        "r.customer_raw",
        "r.customer",
        "r.country_raw",
        "cc.id_country",
        "cc.country",
        "r.channel_raw",
    )
)

unmapped_country_df = (
    ref_with_country_df
    .filter(
        (F.col("destination_country_raw").isNotNull() & F.col("destination_country").isNull())
        | (F.col("country_raw").isNotNull() & F.col("country").isNull())
    )
    .select("destination_country_raw", "destination_country", "country_raw", "country")
    .dropDuplicates()
)

if unmapped_country_df.limit(1).count() > 0:
    print("Country non mappate:")
    display(unmapped_country_df.limit(100))

# COMMAND ----------

# ============================================================
# 7. CHANNEL LOOKUP
# ============================================================

channels_ref_df = _prepare_channels_reference()

ref_with_channel_df = (
    ref_with_country_df.alias("r")
    .join(
        F.broadcast(channels_ref_df).alias("ch"),
        (
            F.upper(F.trim(F.col("r.channel_raw")))
            == F.col("ch._channel_name_key")
        )
        | (
            F.trim(F.col("r.channel_raw"))
            == F.col("ch._channel_id_key")
        ),
        "left",
    )
    .select(
        "r.company",
        "r.destination_country_raw",
        "r.id_destination_country",
        "r.destination_country",
        "r.customer_raw",
        "r.customer",
        "r.country_raw",
        "r.id_country",
        "r.country",
        "r.channel_raw",
        "ch.id_channel",
        "ch.channel",
    )
)

unmapped_channels_df = (
    ref_with_channel_df
    .filter(F.col("channel_raw").isNotNull() & F.col("channel").isNull())
    .groupBy("channel_raw")
    .agg(F.count("*").alias("record_count"))
    .orderBy(F.desc("record_count"), "channel_raw")
)

if unmapped_channels_df.limit(1).count() > 0:
    print("Channel non mappati:")
    display(unmapped_channels_df.limit(100))

# COMMAND ----------

# ============================================================
# 8. OUTPUT FINALE
# ============================================================

final_ref_cc_df = (
    ref_with_channel_df
    .select(
        "company",
        "destination_country_raw",
        "id_destination_country",
        "destination_country",
        "customer_raw",
        "customer",
        "country_raw",
        "id_country",
        "country",
        "channel_raw",
        "id_channel",
        "channel",
    )
    .dropDuplicates(
        [
            "company",
            "destination_country_raw",
            "customer_raw",
        ]
    )
    .withColumn(
        "utility_id",
        F.row_number().over(
            Window.orderBy(
                F.col("company"),
                F.col("destination_country_raw"),
                F.col("customer_raw"),
            )
        ),
    )
    .withColumn("validated", F.lit(True).cast("boolean"))
    .select(
        "utility_id",
        "company",
        "destination_country_raw",
        "id_destination_country",
        "destination_country",
        "customer_raw",
        "customer",
        "country_raw",
        "id_country",
        "country",
        "channel_raw",
        "id_channel",
        "channel",
        "validated",
    )
)

duplicate_grain_df = (
    final_ref_cc_df
    .groupBy("company", "destination_country_raw", "customer_raw")
    .count()
    .filter(F.col("count") > 1)
)

if duplicate_grain_df.limit(1).count() > 0:
    display(duplicate_grain_df)
    raise ValueError(
        "ref_customers_channels non e univoca sul grain "
        "company/sales_country/customer."
    )

print(f"Company univoche         : {final_ref_cc_df.select('company').distinct().count():,}")
print(f"Righe finali reference   : {final_ref_cc_df.count():,}")
display(final_ref_cc_df.limit(100))

# COMMAND ----------

# ============================================================
# 9. SCRITTURA DELTA
# ============================================================

(
    final_ref_cc_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_REF_CC)
)

print(f"Reference creata correttamente: {TARGET_REF_CC}")

# COMMAND ----------

