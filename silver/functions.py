# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver Functions
# MAGIC
# MAGIC Funzioni trasversali usate dagli script del layer Silver.

# COMMAND ----------

# MAGIC %pip install pycountry==24.6.1

# COMMAND ----------

# MAGIC %restart_python 

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import pycountry
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.column import Column

# COMMAND ----------

# ============================================================
# 1. COUNTRY LOOKUP CONFIG
# ============================================================

SOURCE_COUNTRY_DICTIONARY = f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.countries_dictionary"
SOURCE_COUNTRIES = f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.countries"

TARGET_COUNTRIES_STAGING = (
    f"{CATALOG}.{SILVER_SCHEMA}.countries_dictionary_to_validate"
)

print(f"Country dictionary    : {SOURCE_COUNTRY_DICTIONARY}")
print(f"Countries             : {SOURCE_COUNTRIES}")
print(f"Countries staging     : {TARGET_COUNTRIES_STAGING}")

# COMMAND ----------

# ============================================================
# 2. COUNTRY FUNCTIONS
# ============================================================


def _country_to_standard_name(value):
    """Fallback ISO-like basato su pycountry."""
    if value is None or not str(value).strip():
        return None

    try:
        return pycountry.countries.lookup(str(value).strip()).name
    except LookupError:
        return None


country_to_standard_name_udf = F.udf(_country_to_standard_name, "string")


def convert_to_country_iso(df, field_country):
    """
    Converte un campo country raw in nome country standard.

    - cerca country_raw nel PDB countries_dictionary;
    - recupera il nome standard da PDB countries;
    - per valori non censiti tenta pycountry;
    - scrive i valori nuovi in countries_dictionary_to_validate;
    - restituisce: country_raw, id_country, country.
    """
    if field_country not in df.columns:
        raise ValueError(f"Colonna {field_country!r} non presente nel DataFrame.")

    if not spark.catalog.tableExists(SOURCE_COUNTRY_DICTIONARY):
        raise ValueError(f"Country dictionary non trovato: {SOURCE_COUNTRY_DICTIONARY}")

    if not spark.catalog.tableExists(SOURCE_COUNTRIES):
        raise ValueError(f"Countries table non trovata: {SOURCE_COUNTRIES}")

    country_dictionary_df = spark.table(SOURCE_COUNTRY_DICTIONARY)
    countries_df = spark.table(SOURCE_COUNTRIES)

    required_country_columns = {"id_country", "country_raw"}
    required_countries_columns = {"id", "country_name"}

    missing_country_columns = required_country_columns - set(country_dictionary_df.columns)
    missing_countries_columns = required_countries_columns - set(countries_df.columns)

    if missing_country_columns:
        raise ValueError(
            f"Nella tabella {SOURCE_COUNTRY_DICTIONARY} mancano le colonne: "
            f"{sorted(missing_country_columns)}"
        )

    if missing_countries_columns:
        raise ValueError(
            f"Nella tabella {SOURCE_COUNTRIES} mancano le colonne: "
            f"{sorted(missing_countries_columns)}"
        )

    raw_values_df = (
        df
        .select(F.trim(F.col(field_country)).alias("country_raw"))
        .filter(F.col("country_raw").isNotNull() & (F.col("country_raw") != ""))
        .dropDuplicates(["country_raw"])
        .withColumn("country_join_key", F.upper(F.col("country_raw")))
    )

    dictionary_df = (
        country_dictionary_df
        .select(
            F.upper(F.trim(F.col("country_raw"))).alias("country_join_key"),
            F.col("id_country").cast("long").alias("id_country"),
        )
        .filter(F.col("country_join_key").isNotNull())
        .dropDuplicates(["country_join_key", "id_country"])
    )

    ambiguous_dictionary_df = (
        dictionary_df
        .groupBy("country_join_key")
        .agg(F.countDistinct("id_country").alias("id_count"))
        .filter(F.col("id_count") > 1)
    )

    if ambiguous_dictionary_df.limit(1).count() > 0:
        display(ambiguous_dictionary_df)
        raise ValueError("Country dictionary ambiguo: stessa country_raw con piu id.")

    countries_lookup_df = (
        countries_df
        .select(
            F.col("id").cast("long").alias("id_country"),
            F.trim(F.col("country_name")).alias("country"),
        )
        .dropDuplicates(["id_country"])
    )

    mapped_df = (
        raw_values_df.alias("raw")
        .join(F.broadcast(dictionary_df).alias("dict"), "country_join_key", "left")
        .join(F.broadcast(countries_lookup_df).alias("country"), "id_country", "left")
        .select(
            F.col("raw.country_raw"),
            F.col("id_country"),
            F.col("country.country").alias("country"),
        )
    )

    unknown_df = (
        mapped_df
        .filter(F.col("country").isNull())
        .select("country_raw")
        .withColumn("country_join_key", F.upper(F.trim(F.col("country_raw"))))
        .withColumn("country_iso_name", country_to_standard_name_udf(F.col("country_raw")))
    )

    if unknown_df.limit(1).count() > 0:
        countries_by_name_df = (
            countries_df
            .select(
                F.col("id").cast("long").alias("id_country"),
                F.trim(F.col("country_name")).alias("country_name"),
            )
            .withColumn("country_name_join_key", F.upper(F.trim(F.col("country_name"))))
            .dropDuplicates(["country_name_join_key", "id_country"])
        )

        new_rows_df = (
            unknown_df
            .withColumn(
                "country_name_join_key",
                F.upper(F.trim(F.col("country_iso_name"))),
            )
            .join(countries_by_name_df, "country_name_join_key", "left")
            .select(
                "country_join_key",
                "country_raw",
                "country_iso_name",
                F.col("id_country").cast("long").alias("id_country"),
            )
            .withColumn("validated", F.lit(0).cast("int"))
            .withColumn("username", F.lit(None).cast("string"))
            .withColumn("validation_status", F.lit("PENDING"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("source_table", F.lit("landing_*"))
        )

        if not spark.catalog.tableExists(TARGET_COUNTRIES_STAGING):
            (
                new_rows_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(TARGET_COUNTRIES_STAGING)
            )
        else:
            staging_existing_df = (
                spark.table(TARGET_COUNTRIES_STAGING)
                .select("country_join_key")
                .dropDuplicates(["country_join_key"])
            )
            rows_to_append_df = new_rows_df.join(
                staging_existing_df,
                "country_join_key",
                "left_anti",
            )
            if rows_to_append_df.limit(1).count() > 0:
                (
                    rows_to_append_df.write
                    .format("delta")
                    .mode("append")
                    .saveAsTable(TARGET_COUNTRIES_STAGING)
                )

        auto_country_df = (
            new_rows_df
            .select(
                "country_raw",
                F.col("id_country").cast("long").alias("id_country"),
                F.col("country_iso_name").alias("country"),
            )
        )

        mapped_df = (
            mapped_df
            .unionByName(auto_country_df)
            .groupBy("country_raw")
            .agg(
                F.first("id_country", ignorenulls=True).alias("id_country"),
                F.first("country", ignorenulls=True).alias("country"),
            )
        )

    return mapped_df.dropDuplicates(["country_raw"])


def enrich_with_ref_customers_channels(df, reference_table):
    """Arricchisce customer, country e channel e registra le chiavi mancanti."""
    required_input = {
        "company",
        "destination_country_raw",
        "destination_country",
        "customer_raw",
        "channel_raw",
    }
    missing_input = required_input - set(df.columns)
    if missing_input:
        raise ValueError(
            f"Nel DataFrame mancano le colonne: {sorted(missing_input)}"
        )

    if not spark.catalog.tableExists(reference_table):
        raise ValueError(f"Reference non trovata: {reference_table}")

    reference_columns = [
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
    ]
    reference_df = spark.table(reference_table)
    missing_reference = set(reference_columns) - set(reference_df.columns)
    if missing_reference:
        raise ValueError(
            f"Nella reference mancano le colonne: {sorted(missing_reference)}"
        )

    def with_join_keys(source_df):
        return (
            source_df
            .withColumn("_cc_company", F.upper(F.trim(F.col("company"))))
            .withColumn(
                "_cc_destination_country",
                F.upper(F.trim(F.col("destination_country"))),
            )
            .withColumn(
                "_cc_customer_raw",
                F.upper(F.trim(F.col("customer_raw"))),
            )
        )

    reference_prepared_df = with_join_keys(reference_df)

    duplicate_reference_df = (
        reference_prepared_df
        .groupBy("_cc_company", "_cc_destination_country", "_cc_customer_raw")
        .count()
        .filter(F.col("count") > 1)
    )
    duplicate_count = duplicate_reference_df.count()
    if duplicate_count:
        print(f"Chiavi duplicate in {reference_table}: {duplicate_count}")

    quality_score = sum(
        F.when(F.col(column).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        for column in ["customer", "country", "channel"]
    )
    reference_order = []
    if "validated" in reference_df.columns:
        reference_order.append(F.col("validated").desc_nulls_last())
    reference_order.extend([quality_score.desc(), F.col("utility_id").asc_nulls_last()])

    reference_window = Window.partitionBy(
        "_cc_company",
        "_cc_destination_country",
        "_cc_customer_raw",
    ).orderBy(*reference_order)

    reference_unique_df = (
        reference_prepared_df
        .withColumn("_cc_row_number", F.row_number().over(reference_window))
        .filter(F.col("_cc_row_number") == 1)
        .drop("_cc_row_number")
    )

    unique_input_df = (
        df
        .select(
            F.trim(F.col("company")).alias("company"),
            F.trim(F.col("destination_country_raw")).alias(
                "destination_country_raw"
            ),
            F.trim(F.col("destination_country")).alias("destination_country"),
            F.trim(F.col("customer_raw")).alias("customer_raw"),
            F.trim(F.col("channel_raw")).alias("channel_raw"),
        )
        .groupBy("company", "destination_country", "customer_raw")
        .agg(
            F.first("destination_country_raw", ignorenulls=True).alias(
                "destination_country_raw"
            ),
            F.first("channel_raw", ignorenulls=True).alias("channel_raw"),
        )
    )
    unique_input_prepared_df = with_join_keys(unique_input_df)

    key_join_condition = (
        F.col("src._cc_company").eqNullSafe(F.col("ref._cc_company"))
        & F.col("src._cc_destination_country").eqNullSafe(
            F.col("ref._cc_destination_country")
        )
        & F.col("src._cc_customer_raw").eqNullSafe(
            F.col("ref._cc_customer_raw")
        )
    )

    unmatched_df = (
        unique_input_prepared_df.alias("src")
        .join(
            F.broadcast(reference_unique_df).alias("ref"),
            key_join_condition,
            "left",
        )
        .filter(F.col("ref.utility_id").isNull())
        .select("src.*")
    )

    if unmatched_df.limit(1).count() > 0:
        destination_lookup_df = (
            convert_to_country_iso(
                unmatched_df.select("destination_country_raw").dropDuplicates(),
                "destination_country_raw",
            )
            .select(
                F.col("country_raw").alias("_lookup_destination_country_raw"),
                F.col("id_country").cast("long").alias("id_destination_country"),
                F.col("country").alias("_lookup_destination_country"),
            )
        )

        max_utility_id = (
            reference_df
            .agg(F.max(F.col("utility_id")).alias("max_utility_id"))
            .first()["max_utility_id"]
            or 0
        )
        utility_window = Window.orderBy(
            F.coalesce(F.col("company"), F.lit("")),
            F.coalesce(F.col("destination_country"), F.lit("")),
            F.coalesce(F.col("customer_raw"), F.lit("")),
        )

        rows_to_append_df = (
            unmatched_df
            .join(
                F.broadcast(destination_lookup_df),
                F.col("destination_country_raw").eqNullSafe(
                    F.col("_lookup_destination_country_raw")
                ),
                "left",
            )
            .withColumn(
                "utility_id",
                (
                    F.lit(max_utility_id)
                    + F.row_number().over(utility_window)
                ).cast("int"),
            )
            .withColumn(
                "destination_country",
                F.col("_lookup_destination_country"),
            )
            .withColumn("customer", F.lit(None).cast("string"))
            .withColumn("country_raw", F.lit(None).cast("string"))
            .withColumn("id_country", F.lit(None).cast("long"))
            .withColumn("country", F.lit(None).cast("string"))
            .withColumn("id_channel", F.lit(None).cast("long"))
            .withColumn("channel", F.lit(None).cast("string"))
            .select(*reference_columns)
        )

        append_columns = list(reference_columns)
        if "validated" in reference_df.columns:
            rows_to_append_df = rows_to_append_df.withColumn(
                "validated", F.lit(False).cast("boolean")
            )
            append_columns.append("validated")

        rows_to_append_df.select(*append_columns).write.mode("append").saveAsTable(
            reference_table
        )
        print(f"Nuove chiavi accodate a {reference_table}: {rows_to_append_df.count()}")

        reference_df = spark.table(reference_table)
        reference_prepared_df = with_join_keys(reference_df)
        reference_order = []
        if "validated" in reference_df.columns:
            reference_order.append(F.col("validated").desc_nulls_last())
        reference_order.extend(
            [quality_score.desc(), F.col("utility_id").asc_nulls_last()]
        )
        reference_window = Window.partitionBy(
            "_cc_company",
            "_cc_destination_country",
            "_cc_customer_raw",
        ).orderBy(*reference_order)
        reference_unique_df = (
            reference_prepared_df
            .withColumn("_cc_row_number", F.row_number().over(reference_window))
            .filter(F.col("_cc_row_number") == 1)
            .drop("_cc_row_number")
        )

    source_columns = [
        column for column in df.columns if column not in {"customer", "country", "channel"}
    ]
    source_prepared_df = with_join_keys(df)
    final_join_condition = (
        F.col("src._cc_company").eqNullSafe(F.col("ref._cc_company"))
        & F.col("src._cc_destination_country").eqNullSafe(
            F.col("ref._cc_destination_country")
        )
        & F.col("src._cc_customer_raw").eqNullSafe(
            F.col("ref._cc_customer_raw")
        )
    )

    return (
        source_prepared_df.alias("src")
        .join(
            F.broadcast(reference_unique_df).alias("ref"),
            final_join_condition,
            "left",
        )
        .select(
            *[F.col(f"src.`{column}`") for column in source_columns],
            F.col("ref.customer").alias("customer"),
            F.col("ref.country").alias("country"),
            F.col("ref.channel").alias("channel"),
        )
    )


# COMMAND ----------

def normalize_company_name(column: Column) -> Column:
    """
    Normalizza i nomi azienda usati nelle join e nelle dimensioni.

    Portfolio-safe equivalence groups:
    - company_045 / company_group_002 -> company_group_002
    - company_001 / company_070 / company_group_001 -> company_group_001
    - gli altri valori vengono restituiti in maiuscolo e senza spazi esterni
    """
    normalized_company = F.upper(F.trim(column.cast("string")))

    return (
        F.when(
            normalized_company.isin("COMPANY_045", "COMPANY_GROUP_002"),
            F.lit("COMPANY_GROUP_002"),
        )
        .when(
            normalized_company.isin("COMPANY_001", "COMPANY_070", "COMPANY_GROUP_001"),
            F.lit("COMPANY_GROUP_001"),
        )
        .otherwise(normalized_company)
    )
