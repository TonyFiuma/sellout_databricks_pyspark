# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ##Definizione sorgenti e target

# COMMAND ----------

TARGET_REF_COUNTRY_CURRENCY = (
    f"{CATALOG}.{SILVER_SCHEMA}.ref_country_currency"
)

SOURCE_COUNTRIES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.countries"
)

SOURCE_CURRENCIES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.currencies"
)

SOURCE_COUNTRIES_CURRENCIES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.countries_currencies"
)

SOURCE_TIMES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.times"
)

print(f"Source countries           : {SOURCE_COUNTRIES}")
print(f"Source currencies          : {SOURCE_CURRENCIES}")
print(f"Source countries/currencies: {SOURCE_COUNTRIES_CURRENCIES}")
print(f"Source times               : {SOURCE_TIMES}")
print(f"Target                     : {TARGET_REF_COUNTRY_CURRENCY}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Country Currency Reference
# MAGIC
# MAGIC This notebook creates the Silver reference used to associate each country
# MAGIC with its local currency for a specific monthly period.
# MAGIC
# MAGIC The reference is built from the following PDB tables:
# MAGIC
# MAGIC | Target | Source tables | Description |
# MAGIC |---|---|---|
# MAGIC | `silver.ref_country_currency` | `countries`, `currencies`, `countries_currencies`, `times` | Monthly association between country and local currency |
# MAGIC
# MAGIC ## Logical key
# MAGIC
# MAGIC - `year`
# MAGIC - `month`
# MAGIC - `country_name`
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC - `local_currency`

# COMMAND ----------

# MAGIC %md
# MAGIC ##Verifica esistenza delle sorgenti

# COMMAND ----------

source_tables = [
    SOURCE_COUNTRIES,
    SOURCE_CURRENCIES,
    SOURCE_COUNTRIES_CURRENCIES,
    SOURCE_TIMES,
]

for table_name in source_tables:
    exists = spark.catalog.tableExists(table_name)

    print(
        f"{'✓' if exists else '✗'} "
        f"{table_name}: {exists}"
    )

    if not exists:
        raise ValueError(
            f"Source table not found: {table_name}"
        )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM reference_data.master_data.countries
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM reference_data.master_data.currencies
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM reference_data.master_data.countries_currencies
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM reference_data.master_data.times
# MAGIC WHERE month IS NOT NULL
# MAGIC ORDER BY year, month
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Controllo degli schemi

# COMMAND ----------

for table_name in source_tables:
    print(f"\n--- {table_name} ---")

    spark.sql(
        f"DESCRIBE TABLE {table_name}"
    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ref_country_currency
# MAGIC
# MAGIC The table maps each country to its local currency for every available
# MAGIC monthly period.
# MAGIC
# MAGIC The source relationship is:
# MAGIC
# MAGIC `countries_currencies`
# MAGIC → `countries`
# MAGIC → `currencies`
# MAGIC → `times`

# COMMAND ----------

# MAGIC %md
# MAGIC ##Anteprima della join

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     t.id,
# MAGIC     CAST(t.year AS INT)       AS year,
# MAGIC     CAST(t.semester AS INT)   AS semester,
# MAGIC     CAST(t.quarter AS INT)    AS quarter,
# MAGIC     CAST(t.month AS INT)      AS month,
# MAGIC
# MAGIC     c.id,
# MAGIC     TRIM(c.country_name)      AS country_name,
# MAGIC
# MAGIC     cur.id,
# MAGIC     UPPER(TRIM(cur.currency_code)) AS local_currency
# MAGIC
# MAGIC FROM reference_data.master_data.countries_currencies AS cc
# MAGIC
# MAGIC INNER JOIN reference_data.master_data.countries AS c
# MAGIC     ON c.id = cc.id_country
# MAGIC
# MAGIC INNER JOIN reference_data.master_data.currencies AS cur
# MAGIC     ON cur.id = cc.id_currency
# MAGIC
# MAGIC INNER JOIN reference_data.master_data.times AS t
# MAGIC     ON t.id = cc.id_time
# MAGIC
# MAGIC WHERE t.month IS NOT NULL
# MAGIC
# MAGIC ORDER BY
# MAGIC     t.year,
# MAGIC     t.month,
# MAGIC     c.country_name
# MAGIC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creazione della tabella Silver

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_REF_COUNTRY_CURRENCY}
USING DELTA
AS

SELECT DISTINCT
    CAST(t.id AS BIGINT)          AS id_time,
    CAST(t.year AS INT)                AS year,
    CAST(t.semester AS INT)            AS semester,
    CAST(t.quarter AS INT)             AS quarter,
    CAST(t.month AS INT)               AS month,

    CAST(c.id AS BIGINT)       AS id_country,
    TRIM(c.country_name)               AS country_name,
    UPPER(TRIM(c.country_name))        AS country_join_key,

    CAST(cur.id AS BIGINT)    AS id_currency,
    UPPER(TRIM(cur.currency_code))     AS currency_target,
    UPPER(TRIM(cur.currency_code))     AS local_currency,

    CURRENT_TIMESTAMP()                AS reference_created_at

FROM {SOURCE_COUNTRIES_CURRENCIES} AS cc

INNER JOIN {SOURCE_COUNTRIES} AS c
    ON c.id = cc.id_country

INNER JOIN {SOURCE_CURRENCIES} AS cur
    ON cur.id = cc.id_currency

INNER JOIN {SOURCE_TIMES} AS t
    ON t.id = cc.id_time

WHERE t.month IS NOT NULL
  AND CAST(t.month AS INT) BETWEEN 1 AND 12
  AND c.country_name IS NOT NULL
  AND TRIM(c.country_name) <> ''
  AND cur.currency_code IS NOT NULL
  AND TRIM(cur.currency_code) <> ''
""")

print(
    f"✓ Created table: "
    f"{TARGET_REF_COUNTRY_CURRENCY}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Controlli Data Quality

# COMMAND ----------

# MAGIC %md
# MAGIC ###Anteprima della tabella

# COMMAND ----------

ref_country_currency = spark.table(
    TARGET_REF_COUNTRY_CURRENCY
)

display(
    ref_country_currency
    .orderBy(
        "year",
        "month",
        "country_name",
    )
    .limit(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Conteggio righe

# COMMAND ----------

row_count = ref_country_currency.count()

print(
    f"Rows in {TARGET_REF_COUNTRY_CURRENCY}: "
    f"{row_count:,}"
)

if row_count == 0:
    raise ValueError(
        "The country-currency reference is empty."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo della chiave logica

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

duplicates = (
    ref_country_currency

    .groupBy(
        "year",
        "month",
        "country_name",
    )

    .agg(
        F.count("*").alias("row_count"),

        F.countDistinct(
            "local_currency"
        ).alias("currency_count"),

        F.collect_set(
            "local_currency"
        ).alias("currencies"),
    )

    .filter(
        (F.col("row_count") > 1)
        | (F.col("currency_count") > 1)
    )
)

display(duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Blocco errore in caso di duplicati

# COMMAND ----------

duplicate_count = duplicates.count()

print(
    f"Duplicated business keys: "
    f"{duplicate_count:,}"
)

if duplicate_count > 0:
    raise ValueError(
        "Multiple currencies found for the same "
        "year, month and country."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo valori nulli

# COMMAND ----------

null_check = (
    ref_country_currency

    .agg(
        F.count("*").alias("total_rows"),

        F.sum(
            F.when(
                F.col("year").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_year"),

        F.sum(
            F.when(
                F.col("month").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_month"),

        F.sum(
            F.when(
                F.col("country_name").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_country_name"),

        F.sum(
            F.when(
                F.col("local_currency").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_local_currency"),
    )
)

display(null_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validazione valori nulli

# COMMAND ----------

null_result = null_check.first()

mandatory_nulls = (
    null_result["null_year"]
    + null_result["null_month"]
    + null_result["null_country_name"]
    + null_result["null_local_currency"]
)

if mandatory_nulls > 0:
    raise ValueError(
        "Null values found in mandatory columns "
        "of ref_country_currency."
    )

print("✓ Mandatory columns contain no null values.")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo delle valute per paese

# COMMAND ----------

display(
    ref_country_currency

    .select(
        "country_name",
        "local_currency",
    )

    .distinct()

    .orderBy(
        "country_name",
        "local_currency",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validazione finale

# COMMAND ----------

print(
    f"✓ Reference successfully created: "
    f"{TARGET_REF_COUNTRY_CURRENCY}"
)

print(
    f"✓ Total rows: "
    f"{ref_country_currency.count():,}"
)

print(
    "✓ Business key validated: "
    "year + month + country_name"
)

print(
    "✓ Output field available: "
    "local_currency"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     column_name
# MAGIC FROM reference_data.information_schema.columns
# MAGIC WHERE table_schema = 'dbo'
# MAGIC   AND (
# MAGIC       LOWER(column_name) LIKE '%currency%'
# MAGIC       OR LOWER(column_name) LIKE '%valuta%'
# MAGIC   )
# MAGIC ORDER BY table_name, column_name;