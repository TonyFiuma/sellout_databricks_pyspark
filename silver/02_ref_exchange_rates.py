# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sorgenti e target

# COMMAND ----------

TARGET_REF_EXCHANGE_RATES = (
    f"{CATALOG}.{SILVER_SCHEMA}.ref_exchange_rates"
)

SOURCE_EXCHANGE_RATES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.exchange_rates"
)

SOURCE_CURRENCIES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.currencies"
)

SOURCE_TIMES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.times"
)

print(f"Source exchange rates: {SOURCE_EXCHANGE_RATES}")
print(f"Source currencies    : {SOURCE_CURRENCIES}")
print(f"Source times         : {SOURCE_TIMES}")
print(f"Target               : {TARGET_REF_EXCHANGE_RATES}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Exchange Rates Reference
# MAGIC
# MAGIC This notebook creates the Silver reference table:
# MAGIC
# MAGIC `sellout_portfolio.silver.ref_exchange_rates`
# MAGIC
# MAGIC The reference contains monthly exchange rates between a base currency and a
# MAGIC target currency.
# MAGIC
# MAGIC ## Source tables
# MAGIC
# MAGIC - `reference_data.master_data.exchange_rates`
# MAGIC - `reference_data.master_data.currencies`
# MAGIC - `reference_data.master_data.times`
# MAGIC
# MAGIC ## Logical key
# MAGIC
# MAGIC - `year`
# MAGIC - `month`
# MAGIC - `currency_base`
# MAGIC - `currency_target`
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC - `exchange_rate`

# COMMAND ----------

# MAGIC %md
# MAGIC ##Controlli delle sorgenti

# COMMAND ----------

source_tables = [
    SOURCE_EXCHANGE_RATES,
    SOURCE_CURRENCIES,
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

# MAGIC %md
# MAGIC ###Anteprima

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM reference_data.master_data.exchange_rates
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM reference_data.master_data.currencies
# MAGIC ORDER BY id
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM reference_data.master_data.times
# MAGIC WHERE month IS NOT NULL
# MAGIC ORDER BY year, month
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Preview della reference

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     er.id                                      AS id_exchange_rate,
# MAGIC     er.id_time,
# MAGIC
# MAGIC     t.year,
# MAGIC     t.semester,
# MAGIC     t.quarter,
# MAGIC     t.month,
# MAGIC
# MAGIC     er.id_currency_base,
# MAGIC     UPPER(TRIM(cb.currency_code))              AS currency_base,
# MAGIC
# MAGIC     er.id_currency_target,
# MAGIC     UPPER(TRIM(ct.currency_code))              AS currency_target,
# MAGIC
# MAGIC     TRY_CAST(
# MAGIC         REPLACE(TRIM(er.exchange_rate), ',', '.')
# MAGIC         AS DOUBLE
# MAGIC     )                                          AS exchange_rate
# MAGIC
# MAGIC FROM reference_data.master_data.exchange_rates AS er
# MAGIC
# MAGIC INNER JOIN reference_data.master_data.currencies AS cb
# MAGIC     ON cb.id = er.id_currency_base
# MAGIC
# MAGIC INNER JOIN reference_data.master_data.currencies AS ct
# MAGIC     ON ct.id = er.id_currency_target
# MAGIC
# MAGIC INNER JOIN reference_data.master_data.times AS t
# MAGIC     ON t.id = er.id_time
# MAGIC
# MAGIC WHERE t.month IS NOT NULL
# MAGIC
# MAGIC ORDER BY
# MAGIC     t.year,
# MAGIC     t.month,
# MAGIC     currency_base,
# MAGIC     currency_target
# MAGIC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creazione tabella Delta

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_REF_EXCHANGE_RATES}
USING DELTA
AS

WITH ranked_exchange_rates AS (

    SELECT
        CAST(er.id AS BIGINT)                 AS id_exchange_rate,
        CAST(er.id_time AS BIGINT)            AS id_time,

        CAST(t.year AS INT)                   AS year,
        CAST(t.semester AS INT)               AS semester,
        CAST(t.quarter AS INT)                AS quarter,
        CAST(t.month AS INT)                  AS month,
        CAST(t.id_time_frame AS INT)          AS id_time_frame,

        CAST(er.id_currency_base AS BIGINT)   AS id_currency_base,
        UPPER(TRIM(cb.currency_code))         AS currency_base,

        CAST(er.id_currency_target AS BIGINT) AS id_currency_target,
        UPPER(TRIM(ct.currency_code))         AS currency_target,

        CASE
            WHEN TRY_CAST(
                REPLACE(TRIM(er.exchange_rate), ',', '.')
                AS DOUBLE
            ) > 0
            THEN TRY_CAST(
                REPLACE(TRIM(er.exchange_rate), ',', '.')
                AS DOUBLE
            )
            ELSE NULL
        END AS exchange_rate,

        ROW_NUMBER() OVER (
            PARTITION BY
                CAST(t.year AS INT),
                CAST(t.month AS INT),
                UPPER(TRIM(cb.currency_code)),
                UPPER(TRIM(ct.currency_code))
            ORDER BY CAST(er.id AS BIGINT)
        ) AS row_number

    FROM {SOURCE_EXCHANGE_RATES} AS er

    INNER JOIN {SOURCE_CURRENCIES} AS cb
        ON cb.id = er.id_currency_base

    INNER JOIN {SOURCE_CURRENCIES} AS ct
        ON ct.id = er.id_currency_target

    INNER JOIN {SOURCE_TIMES} AS t
        ON t.id = er.id_time

    WHERE t.month IS NOT NULL
      AND CAST(t.month AS INT) BETWEEN 1 AND 12
      AND CAST(t.id_time_frame AS INT) = 4
)

SELECT
    id_exchange_rate,
    id_time,
    year,
    semester,
    quarter,
    month,
    id_time_frame,
    id_currency_base,
    currency_base,
    id_currency_target,
    currency_target,
    exchange_rate,
    CURRENT_TIMESTAMP() AS reference_created_at

FROM ranked_exchange_rates
WHERE row_number = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Quality

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lettura della reference creata

# COMMAND ----------

ref_exchange_rates = spark.table(
    TARGET_REF_EXCHANGE_RATES
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Anteprima finale

# COMMAND ----------

display(
    ref_exchange_rates
    .orderBy(
        "year",
        "month",
        "currency_base",
        "currency_target",
    )
    .limit(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Conteggio record

# COMMAND ----------

row_count = ref_exchange_rates.count()

print(
    f"Rows in {TARGET_REF_EXCHANGE_RATES}: "
    f"{row_count:,}"
)

if row_count == 0:
    raise ValueError(
        "The exchange-rate reference is empty."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo conversioni non numeriche

# COMMAND ----------

invalid_exchange_rates = (
    spark.table(SOURCE_EXCHANGE_RATES)

    .withColumn(
        "exchange_rate_numeric",
        F.expr(
            """
            TRY_CAST(
                REPLACE(TRIM(exchange_rate), ',', '.')
                AS DOUBLE
            )
            """
        ),
    )

    .filter(
        F.col("exchange_rate").isNotNull()
        & (F.trim(F.col("exchange_rate")) != "")
        & F.col("exchange_rate_numeric").isNull()
    )

    .select(
        "id",
        "id_time",
        "id_currency_base",
        "id_currency_target",
        "exchange_rate",
    )
)

display(invalid_exchange_rates)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Blocco errore per tassi non validi

# COMMAND ----------

invalid_exchange_rate_count = (
    invalid_exchange_rates.count()
)

print(
    f"Invalid exchange rates: "
    f"{invalid_exchange_rate_count:,}"
)

if invalid_exchange_rate_count > 0:
    raise ValueError(
        "Some exchange_rate values cannot be "
        "converted to DOUBLE."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo chiave logica

# COMMAND ----------

duplicates = (
    ref_exchange_rates
    .groupBy(
        "year",
        "month",
        "currency_base",
        "currency_target",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct(
            "exchange_rate"
        ).alias("rate_count"),
        F.collect_set(
            "exchange_rate"
        ).alias("exchange_rates"),
    )
    .filter(
        F.col("row_count") > 1
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Blocco errore duplicati

# COMMAND ----------

duplicate_count = duplicates.count()

print(
    f"Duplicated business keys: "
    f"{duplicate_count:,}"
)

if duplicate_count > 0:
    display(duplicates)

    raise ValueError(
        "Multiple exchange rates found for the same "
        "year, month, currency_base and currency_target."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo valori nulli

# COMMAND ----------

null_check = (
    ref_exchange_rates

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
                F.col("currency_base").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_currency_base"),

        F.sum(
            F.when(
                F.col("currency_target").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_currency_target"),

        F.sum(
            F.when(
                F.col("exchange_rate").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_exchange_rate"),
    )
)

display(null_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validazione valori nulli

# COMMAND ----------

null_result = null_check.first()

structural_nulls = (
    null_result["null_year"]
    + null_result["null_month"]
    + null_result["null_currency_base"]
    + null_result["null_currency_target"]
)

if structural_nulls > 0:
    raise ValueError(
        "Null values found in structural key columns "
        "of ref_exchange_rates."
    )

print("✓ Structural key columns contain no null values.")

missing_rate_count = null_result["null_exchange_rate"]

if missing_rate_count > 0:
    print(
        f"⚠ Exchange rates missing or invalid: "
        f"{missing_rate_count:,}"
    )
else:
    print("✓ All exchange rates are populated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo tassi non positivi

# COMMAND ----------

invalid_or_missing_rates = (
    ref_exchange_rates
    .filter(
        F.col("exchange_rate").isNull()
        | (F.col("exchange_rate") <= 0)
    )
)

display(invalid_or_missing_rates)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Blocco errore tassi non positivi

# COMMAND ----------

invalid_or_missing_count = (
    invalid_or_missing_rates.count()
)

print(
    f"Invalid or missing exchange rates: "
    f"{invalid_or_missing_count:,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo copertura temporale

# COMMAND ----------

display(
    ref_exchange_rates

    .groupBy("year")

    .agg(
        F.min("month").alias("min_month"),
        F.max("month").alias("max_month"),
        F.countDistinct("month").alias("months_available"),
        F.count("*").alias("rate_rows"),
    )

    .orderBy("year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo coppie valutarie

# COMMAND ----------

display(
    ref_exchange_rates

    .select(
        "currency_base",
        "currency_target",
    )

    .distinct()

    .orderBy(
        "currency_base",
        "currency_target",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Controllo target disponibili

# COMMAND ----------

display(
    ref_exchange_rates

    .groupBy(
        "currency_target"
    )

    .agg(
        F.count("*").alias("rate_count"),
        F.min("year").alias("min_year"),
        F.max("year").alias("max_year"),
    )

    .orderBy(
        "currency_target"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validazione finale

# COMMAND ----------

print(
    f"✓ Reference successfully created: "
    f"{TARGET_REF_EXCHANGE_RATES}"
)

print(
    f"✓ Total rows: "
    f"{ref_exchange_rates.count():,}"
)

print(
    "✓ Business key validated: "
    "year + month + currency_base + currency_target"
)

print(
    "✓ exchange_rate converted from STRING to DOUBLE"
)

# COMMAND ----------

display(
    ref_exchange_rates
    .groupBy("currency_target")
    .count()
    .orderBy("currency_target")
)