# Databricks notebook source
# MAGIC %md
# MAGIC # FX Rates Normalization
# MAGIC
# MAGIC Questo notebook reimplementa in PySpark la logica definita in
# MAGIC `fx_rates_normalization.qmd`.
# MAGIC
# MAGIC La pipeline:
# MAGIC
# MAGIC 1. normalizza la direzione dei tassi originali;
# MAGIC 2. gestisce ridenominazioni, unioni monetarie e sostituzioni di valuta;
# MAGIC 3. applica la back-calculation alle serie storiche;
# MAGIC 4. genera tassi inversi e cross-rate tramite USD;
# MAGIC 5. mantiene separata la gestione delle valute legacy;
# MAGIC 6. produce granularità mensile, trimestrale, semestrale e annuale.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from functools import reduce


# COMMAND ----------

# MAGIC %run ./config
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurazione
# MAGIC

# COMMAND ----------

SOURCE_REF_EXCHANGE_RATES = f"{CATALOG}.{SILVER_SCHEMA}.ref_exchange_rates"

SOURCE_COUNTRIES_CURRENCIES = (
    f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.countries_currencies"
)
SOURCE_COUNTRIES = f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.countries"
SOURCE_CURRENCIES = f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.currencies"
SOURCE_TIMES = f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.times"

TARGET_REF_FX_NORMALIZED = f"{CATALOG}.{SILVER_SCHEMA}.ref_fx_normalized"
TARGET_REF_FX_REPORTING = f"{CATALOG}.{SILVER_SCHEMA}.ref_fx_reporting"
TARGET_REF_FX_LEGACY = f"{CATALOG}.{SILVER_SCHEMA}.ref_fx_legacy"

PIVOT_CURRENCY = "USD"

print(f"Source reference     : {SOURCE_REF_EXCHANGE_RATES}")
print(f"Country/currency map : {SOURCE_COUNTRIES_CURRENCIES}")
print(f"Countries            : {SOURCE_COUNTRIES}")
print(f"Currencies           : {SOURCE_CURRENCIES}")
print(f"Times                : {SOURCE_TIMES}")
print(f"Normalized reference : {TARGET_REF_FX_NORMALIZED}")
print(f"Reporting reference  : {TARGET_REF_FX_REPORTING}")
print(f"Legacy reference     : {TARGET_REF_FX_LEGACY}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Eventi di discontinuità valutaria
# MAGIC
# MAGIC Il fattore è espresso come:
# MAGIC
# MAGIC `1 unità successor = factor unità predecessor`
# MAGIC
# MAGIC Questa configurazione è l'unico input manuale della pipeline e deve essere
# MAGIC aggiornata quando viene rilevato un nuovo evento.
# MAGIC

# COMMAND ----------

redenomination_events_data = [
    ("BYR", "BYN", 10000.0, "redenomination"),
    ("HRK", "EUR", 7.5345, "currency_union"),
    ("BGN", "EUR", 1.95583, "currency_union"),
    ("ANG", "XCG", 1.0, "currency_replacement"),
    ("SLL", "SLE", 1000.0, "redenomination"),
    ("STD", "STN", 1000.0, "redenomination"),
    ("MRO", "MRU", 10.0, "redenomination"),
    ("VEF", "VES", 100000.0, "redenomination"),
    ("VES", "VES", 7324980.0, "redenomination"),
    ("SYP", "SYP", 100.0, "redenomination"),
    ("USD", "ZWL", None, "currency_reintroduction"),
    ("ZWL", "ZWG", 2498.7242, "currency_replacement"),
]

redenomination_events = spark.createDataFrame(
    redenomination_events_data,
    schema=(
        "currency_predecessor string, "
        "currency_successor string, "
        "factor double, "
        "event_type string"
    ),
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lettura e validazione della sorgente
# MAGIC

# COMMAND ----------

if not spark.catalog.tableExists(SOURCE_REF_EXCHANGE_RATES):
    raise ValueError(f"Source reference not found: {SOURCE_REF_EXCHANGE_RATES}")

ref_exchange_rates = spark.table(SOURCE_REF_EXCHANGE_RATES)

required_columns = {
    "year",
    "month",
    "currency_base",
    "currency_target",
    "exchange_rate",
}

missing_columns = sorted(required_columns - set(ref_exchange_rates.columns))

if missing_columns:
    raise ValueError(
        "Missing columns in ref_exchange_rates: "
        + ", ".join(missing_columns)
    )

print(f"✓ Source rows: {ref_exchange_rates.count():,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inversione, pulizia e selezione delle serie
# MAGIC
# MAGIC Come nel file di riferimento, `currency_base` e `currency_target` vengono
# MAGIC invertite per ottenere tassi espressi come unità di valuta target per una unità
# MAGIC di valuta base. I tassi nulli, uguali a zero o negativi vengono scartati.
# MAGIC

# COMMAND ----------

db_fx = (
    ref_exchange_rates
    .select(
        F.col("year").cast("int").alias("year"),
        F.col("month").cast("int").alias("month"),
        F.upper(F.trim(F.col("currency_target"))).alias("currency_base"),
        F.upper(F.trim(F.col("currency_base"))).alias("currency_target"),
        F.col("exchange_rate").cast("double").alias("exchange_rate"),
    )
    .withColumn("source", F.lit("df_original"))
    .dropDuplicates()
)

invalid_fx = db_fx.filter(
    F.col("exchange_rate").isNull()
    | (F.col("exchange_rate") <= 0)
)

db_fx_clean = db_fx.filter(
    F.col("exchange_rate").isNotNull()
    & (F.col("exchange_rate") > 0)
)

print(f"Invalid FX rows excluded: {invalid_fx.count():,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Costruzione della relazione Paese–valuta
# MAGIC
# MAGIC `country_name` non è presente in `ref_exchange_rates`. Come nella funzione R
# MAGIC `sql_query_countries_currencies()`, viene ricavato dalla relazione PDB
# MAGIC `countries_currencies`, collegata alle dimensioni `countries`, `currencies`
# MAGIC e `times`.
# MAGIC

# COMMAND ----------

country_currency_source_tables = [
    SOURCE_COUNTRIES_CURRENCIES,
    SOURCE_COUNTRIES,
    SOURCE_CURRENCIES,
    SOURCE_TIMES,
]

for table_name in country_currency_source_tables:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(
            f"Country/currency source table not found: {table_name}"
        )

countries_currencies = spark.table(SOURCE_COUNTRIES_CURRENCIES)
countries = spark.table(SOURCE_COUNTRIES)
currencies = spark.table(SOURCE_CURRENCIES)
times = spark.table(SOURCE_TIMES)


def first_existing_column(df, candidates, dataframe_name):
    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    raise ValueError(
        f"None of the expected columns {candidates} "
        f"was found in {dataframe_name}. "
        f"Available columns: {df.columns}"
    )


cc_country_id = first_existing_column(
    countries_currencies,
    ["id_country", "country_id"],
    "countries_currencies",
)
cc_currency_id = first_existing_column(
    countries_currencies,
    ["id_currency", "currency_id"],
    "countries_currencies",
)
cc_time_id = first_existing_column(
    countries_currencies,
    ["id_time", "time_id"],
    "countries_currencies",
)

country_id = first_existing_column(
    countries,
    ["id", "id_country", "country_id"],
    "countries",
)
country_name = first_existing_column(
    countries,
    ["country_name", "country", "name"],
    "countries",
)

currency_id = first_existing_column(
    currencies,
    ["id", "id_currency", "currency_id"],
    "currencies",
)
currency_code = first_existing_column(
    currencies,
    ["currency_code", "code"],
    "currencies",
)

time_id = first_existing_column(
    times,
    ["id", "id_time", "time_id"],
    "times",
)
time_year = first_existing_column(times, ["year"], "times")
time_month = first_existing_column(times, ["month"], "times")


# COMMAND ----------

country_currency_df = (
    countries_currencies.alias("cc")
    .join(
        countries.alias("c"),
        F.col(f"cc.{cc_country_id}") == F.col(f"c.{country_id}"),
        "inner",
    )
    .join(
        currencies.alias("cu"),
        F.col(f"cc.{cc_currency_id}") == F.col(f"cu.{currency_id}"),
        "inner",
    )
    .join(
        times.alias("t"),
        F.col(f"cc.{cc_time_id}") == F.col(f"t.{time_id}"),
        "inner",
    )
    .filter(F.col(f"t.{time_month}").isNotNull())
    .select(
        F.col(f"t.{time_year}").cast("int").alias("year"),
        F.col(f"t.{time_month}").cast("int").alias("month"),
        F.upper(
            F.trim(F.col(f"cu.{currency_code}"))
        ).alias("currency_code"),
        F.trim(F.col(f"c.{country_name}")).alias("country_name"),
    )
    .filter(F.col("currency_code").isNotNull())
    .filter(F.col("country_name").isNotNull())
    .dropDuplicates()
)

if country_currency_df.limit(1).count() == 0:
    raise ValueError("The country/currency relation is empty.")

display(
    country_currency_df
    .orderBy("year", "month", "country_name", "currency_code")
    .limit(100)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Associazione dei tassi ai Paesi
# MAGIC
# MAGIC La join replica la chiave utilizzata nel file QMD:
# MAGIC
# MAGIC `year + month + currency_target = currency_code`.
# MAGIC

# COMMAND ----------

fx_country = (
    db_fx_clean.alias("fx")
    .filter(F.col("currency_base").isin("EUR", "USD"))
    .join(
        country_currency_df.alias("cc"),
        on=[
            F.col("fx.year") == F.col("cc.year"),
            F.col("fx.month") == F.col("cc.month"),
            F.col("fx.currency_target") == F.col("cc.currency_code"),
        ],
        how="left",
    )
    .select(
        F.col("fx.year").alias("year"),
        F.col("fx.month").alias("month"),
        F.col("fx.currency_base").alias("currency_base"),
        F.col("fx.currency_target").alias("currency_target"),
        F.col("cc.country_name").alias("country_name"),
        F.col("fx.exchange_rate").alias("exchange_rate"),
        F.col("fx.source").alias("source"),
    )
    .filter(F.col("country_name").isNotNull())
    .filter(
        ~(
            (F.col("year") == 2018)
            & (F.col("month") == 8)
            & (F.col("currency_target") == "VEF")
        )
    )
    .dropDuplicates()
)

display(
    fx_country
    .orderBy("country_name", "year", "month", "currency_base")
    .limit(100)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Costruzione della tabella delle ridenominazioni
# MAGIC
# MAGIC Per ogni Paese con più valute nella serie storica viene individuato l'ultimo
# MAGIC periodo della valuta predecessor e la valuta successiva. Gli eventi con lo
# MAGIC stesso codice ISO, non rilevabili automaticamente, vengono aggiunti
# MAGIC esplicitamente.
# MAGIC

# COMMAND ----------

currency_last_period = (
    fx_country
    .groupBy("country_name", "currency_target")
    .agg(
        F.max(
            F.make_date(
                F.col("year"),
                F.col("month"),
                F.lit(1),
            )
        ).alias("last_period")
    )
)

currency_sequence_window = Window.partitionBy(
    "country_name"
).orderBy(
    "last_period",
    "currency_target",
)

detected_switches = (
    currency_last_period
    .withColumn(
        "currency_successor",
        F.lead("currency_target").over(currency_sequence_window),
    )
    .filter(F.col("currency_successor").isNotNull())
    .select(
        F.year("last_period").alias("year"),
        F.month("last_period").alias("month"),
        "country_name",
        F.col("currency_target").alias("currency_predecessor"),
        "currency_successor",
    )
)

manual_same_code_switches = spark.createDataFrame(
    [
        (2021, 9, "Venezuela", "VES", "VES"),
        (2025, 12, "Syria", "SYP", "SYP"),
    ],
    schema=(
        "year int, month int, country_name string, "
        "currency_predecessor string, currency_successor string"
    ),
)

tbl_redenomination = (
    detected_switches
    .unionByName(manual_same_code_switches)
    .join(
        F.broadcast(redenomination_events),
        on=["currency_predecessor", "currency_successor"],
        how="left",
    )
)

unconfigured_events = tbl_redenomination.filter(
    F.col("event_type").isNull()
)

if unconfigured_events.limit(1).count() > 0:
    print(
        "⚠ Currency switches without a configured event were found. "
        "They will not be used for back-calculation."
    )
    display(unconfigured_events)

tbl_redenomination = tbl_redenomination.filter(
    F.col("event_type").isNotNull()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tassi di ridenominazione e back-calculation
# MAGIC
# MAGIC Per gli eventi multipli sullo stesso Paese il fattore cumulato è il prodotto
# MAGIC dei fattori dall'evento corrente all'ultimo evento disponibile.
# MAGIC

# COMMAND ----------

factor_window = (
    Window
    .partitionBy("country_name", "currency_successor")
    .orderBy("year", "month")
    .rowsBetween(Window.currentRow, Window.unboundedFollowing)
)

latest_successor_window = (
    Window
    .partitionBy("country_name")
    .orderBy(
        F.col("year").desc(),
        F.col("month").desc(),
    )
)

redenomination_with_factor = (
    tbl_redenomination
    .withColumn(
        "factor_cum",
        F.exp(
            F.sum(F.log(F.col("factor"))).over(factor_window)
        ),
    )
    .withColumn(
        "_latest_successor",
        F.first("currency_successor").over(latest_successor_window),
    )
    .withColumn(
        "currency_successor",
        F.col("_latest_successor"),
    )
    .drop("_latest_successor")
)

predecessor_periods = (
    fx_country
    .select(
        "year",
        "month",
        "country_name",
        F.col("currency_target").alias("currency_predecessor"),
    )
    .distinct()
)

fx_country_redenomination = (
    redenomination_with_factor.alias("r")
    .join(
        predecessor_periods.alias("p"),
        on=[
            F.col("r.country_name") == F.col("p.country_name"),
            F.col("r.currency_predecessor")
            == F.col("p.currency_predecessor"),
        ],
        how="inner",
    )
    .filter(
        F.make_date(
            F.col("p.year"),
            F.col("p.month"),
            F.lit(1),
        )
        <= F.make_date(
            F.col("r.year"),
            F.col("r.month"),
            F.lit(1),
        )
    )
    .filter(F.col("r.factor_cum").isNotNull())
    .select(
        F.col("p.year").alias("year"),
        F.col("p.month").alias("month"),
        F.col("r.currency_predecessor").alias("currency_base"),
        F.col("r.currency_successor").alias("currency_target"),
        F.col("r.country_name").alias("country_name"),
        (F.lit(1.0) / F.col("r.factor_cum")).alias("exchange_rate"),
        F.col("r.event_type").alias("event_type"),
        F.lit("df_redenomination").alias("source"),
    )
    .dropDuplicates()
)


# COMMAND ----------

redenomination_for_join = (
    fx_country_redenomination
    .select(
        "year",
        "month",
        "country_name",
        F.col("currency_base").alias("currency_predecessor"),
        F.col("currency_target").alias("currency_successor"),
        F.col("exchange_rate").alias("redenomination_factor"),
        "event_type",
    )
)

fx_country_norm = (
    fx_country.alias("f")
    .join(
        redenomination_for_join.alias("r"),
        on=[
            F.col("f.year") == F.col("r.year"),
            F.col("f.month") == F.col("r.month"),
            F.col("f.country_name") == F.col("r.country_name"),
            F.col("f.currency_target")
            == F.col("r.currency_predecessor"),
        ],
        how="left",
    )
    .select(
        F.col("f.year").alias("year"),
        F.col("f.month").alias("month"),
        F.col("f.currency_base").alias("currency_base"),
        F.coalesce(
            F.col("r.currency_successor"),
            F.col("f.currency_target"),
        ).alias("currency_target"),
        F.col("f.country_name").alias("country_name"),
        F.coalesce(
            F.col("f.exchange_rate")
            * F.col("r.redenomination_factor"),
            F.col("f.exchange_rate"),
        ).alias("exchange_rate"),
        F.col("r.event_type").alias("event_type"),
        F.col("f.source").alias("source"),
    )
    .filter(F.col("currency_base") != F.col("currency_target"))
    .dropDuplicates()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tassi inversi
# MAGIC

# COMMAND ----------

fx_norm_lookup = (
    fx_country_norm
    .select(
        "year",
        "month",
        "currency_target",
        "country_name",
        "event_type",
    )
    .dropDuplicates()
)

fx_inverted = (
    fx_country_norm.alias("f")
    .join(
        fx_norm_lookup.alias("l"),
        on=[
            F.col("f.year") == F.col("l.year"),
            F.col("f.month") == F.col("l.month"),
            F.col("f.currency_base") == F.col("l.currency_target"),
        ],
        how="left",
    )
    .filter(
        ~(
            (F.col("f.currency_target") == "EUR")
            & (F.col("f.currency_base") == "USD")
        )
    )
    .filter(
        ~(
            (F.col("f.currency_target") == "USD")
            & (F.col("f.currency_base") == "EUR")
        )
    )
    .filter(
        ~(
            (
                F.coalesce(
                    F.col("f.event_type"),
                    F.lit(""),
                )
                == "currency_union"
            )
            & F.col("f.currency_target").isin("EUR", "USD")
        )
    )
    .filter(
        ~(
            (
                F.coalesce(
                    F.col("l.event_type"),
                    F.lit(""),
                )
                == "currency_union"
            )
            & F.col("f.currency_base").isin("EUR", "USD")
        )
    )
    .select(
        F.col("f.year").alias("year"),
        F.col("f.month").alias("month"),
        F.col("f.currency_target").alias("currency_base"),
        F.col("f.currency_base").alias("currency_target"),
        F.col("l.country_name").alias("country_name"),
        (F.lit(1.0) / F.col("f.exchange_rate")).alias("exchange_rate"),
        F.coalesce(
            F.col("f.event_type"),
            F.col("l.event_type"),
        ).alias("event_type"),
        F.lit("df_inverted").alias("source"),
    )
    .dropDuplicates()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cross-rate tramite USD
# MAGIC
# MAGIC La triangolazione segue la formula:
# MAGIC
# MAGIC `cross(A/B) = rate(B/USD) / rate(A/USD)`
# MAGIC

# COMMAND ----------

fx_norm_usd = (
    fx_country_norm
    .filter(F.col("currency_base") == PIVOT_CURRENCY)
    .select(
        "year",
        "month",
        "currency_target",
        "country_name",
        "exchange_rate",
        "event_type",
    )
    .dropDuplicates()
)

fx_cross_pairs = (
    fx_norm_usd.alias("b")
    .join(
        fx_norm_usd.alias("t"),
        on=[
            F.col("b.year") == F.col("t.year"),
            F.col("b.month") == F.col("t.month"),
        ],
        how="inner",
    )
    .filter(F.col("b.currency_target") != F.col("t.currency_target"))
    .filter(~F.col("b.currency_target").isin("EUR", "USD"))
    .filter(
        ~(
            F.col("t.currency_target").isin("EUR", "USD")
            & (
                F.coalesce(
                    F.col("t.event_type"),
                    F.lit(""),
                )
                != "currency_union"
            )
        )
    )
)

fx_all_combination = (
    fx_cross_pairs
    .select(
        F.col("b.year").alias("year"),
        F.col("b.month").alias("month"),
        F.col("b.currency_target").alias("currency_base"),
        F.col("t.currency_target").alias("currency_target"),
        F.col("t.country_name").alias("country_name"),
        (
            F.col("t.exchange_rate")
            / F.col("b.exchange_rate")
        ).alias("exchange_rate"),
        F.when(
            F.col("b.event_type").isNotNull()
            & F.col("t.event_type").isNotNull(),
            F.lit("synthetic_cross"),
        )
        .otherwise(
            F.coalesce(
                F.col("t.event_type"),
                F.col("b.event_type"),
            )
        )
        .alias("event_type"),
        F.lit("df_all_combination").alias("source"),
    )
    .filter(F.col("exchange_rate").isNotNull())
    .dropDuplicates()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Valute legacy
# MAGIC
# MAGIC Le valute non più attive vengono calcolate a partire dai dati originali,
# MAGIC senza applicare la normalizzazione della valuta successor.
# MAGIC

# COMMAND ----------

legacy_currency_periods = (
    db_fx_clean
    .select(
        "year",
        "month",
        F.col("currency_target").alias("currency"),
    )
    .distinct()
)

legacy_pairs = (
    legacy_currency_periods.alias("b")
    .join(
        legacy_currency_periods.alias("t"),
        on=[
            F.col("b.year") == F.col("t.year"),
            F.col("b.month") == F.col("t.month"),
        ],
        how="inner",
    )
    .filter(F.col("b.currency") != F.col("t.currency"))
)

legacy_usd_rates = (
    db_fx_clean
    .filter(F.col("currency_base") == PIVOT_CURRENCY)
    .select(
        "year",
        "month",
        F.col("currency_target").alias("currency"),
        F.col("exchange_rate").alias("rate_usd"),
    )
    .dropDuplicates()
)

fx_legacy_combination = (
    legacy_pairs
    .join(
        legacy_usd_rates.alias("rb"),
        on=[
            F.col("b.year") == F.col("rb.year"),
            F.col("b.month") == F.col("rb.month"),
            F.col("b.currency") == F.col("rb.currency"),
        ],
        how="left",
    )
    .join(
        legacy_usd_rates.alias("rt"),
        on=[
            F.col("b.year") == F.col("rt.year"),
            F.col("b.month") == F.col("rt.month"),
            F.col("t.currency") == F.col("rt.currency"),
        ],
        how="left",
    )
    .join(
        redenomination_events
        .select("currency_predecessor", "event_type")
        .dropDuplicates()
        .alias("e"),
        on=F.col("b.currency") == F.col("e.currency_predecessor"),
        how="left",
    )
    .select(
        F.col("b.year").alias("year"),
        F.col("b.month").alias("month"),
        F.col("b.currency").alias("currency_base"),
        F.col("t.currency").alias("currency_target"),
        (
            F.when(
                F.col("t.currency") == PIVOT_CURRENCY,
                F.lit(1.0),
            )
            .otherwise(F.col("rt.rate_usd"))
            /
            F.when(
                F.col("b.currency") == PIVOT_CURRENCY,
                F.lit(1.0),
            )
            .otherwise(F.col("rb.rate_usd"))
        ).alias("exchange_rate"),
        F.col("e.event_type").alias("event_type"),
        F.lit("df_legacy").alias("source"),
    )
    .filter(F.col("exchange_rate").isNotNull())
    .dropDuplicates()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Merge finale
# MAGIC

# COMMAND ----------

fx_normalized = (
    fx_country_norm
    .unionByName(fx_inverted)
    .unionByName(fx_all_combination)
    .unionByName(fx_country_redenomination)
)

normalized_non_redenomination_bases = (
    fx_normalized
    .filter(F.col("source") != "df_redenomination")
    .select("currency_base")
    .distinct()
)

fx_legacy = (
    fx_legacy_combination
    .join(
        normalized_non_redenomination_bases,
        on="currency_base",
        how="left_anti",
    )
    .filter(~F.col("currency_base").isin("EUR", "USD"))
    .filter(
        ~(
            F.col("currency_target").isin("EUR", "USD")
            & (F.col("event_type") == "currency_union")
        )
    )
    .select(
        "year",
        "month",
        "currency_base",
        "currency_target",
        "exchange_rate",
        "source",
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Aggregazione per time frame
# MAGIC
# MAGIC Ogni tasso mensile viene reso disponibile anche a livello trimestrale,
# MAGIC semestrale e annuale. L'aggregazione avviene dopo la back-calculation.
# MAGIC

# COMMAND ----------

def build_time_frames(df):
    monthly = (
        df
        .withColumn("time_frame", F.lit("month"))
        .withColumn("period_value", F.col("month"))
    )

    quarterly = (
        df
        .withColumn("time_frame", F.lit("quarter"))
        .withColumn(
            "period_value",
            (F.floor((F.col("month") - 1) / 3) + 1).cast("int"),
        )
    )

    semester = (
        df
        .withColumn("time_frame", F.lit("semester"))
        .withColumn(
            "period_value",
            F.when(F.col("month") <= 6, F.lit(1))
            .otherwise(F.lit(2)),
        )
    )

    annual = (
        df
        .withColumn("time_frame", F.lit("year"))
        .withColumn("period_value", F.col("year"))
    )

    return reduce(
        lambda left, right: left.unionByName(right),
        [monthly, quarterly, semester, annual],
    )


fx_long = build_time_frames(fx_normalized)
fx_long_legacy = build_time_frames(fx_legacy)


# COMMAND ----------

ref_fx_normalized_final = (
    fx_long
    .groupBy(
        "year",
        "period_value",
        "time_frame",
        "currency_base",
        "currency_target",
        "country_name",
    )
    .agg(F.avg("exchange_rate").alias("exchange_rate"))
    .withColumn("reference_created_at", F.current_timestamp())
)

ref_fx_reporting_final = (
    fx_long
    .filter(F.col("currency_target").isin("EUR", "USD"))
    .filter(
        F.coalesce(F.col("event_type"), F.lit(""))
        != "currency_union"
    )
    .groupBy(
        "year",
        "period_value",
        "time_frame",
        "currency_base",
        "currency_target",
    )
    .agg(F.avg("exchange_rate").alias("exchange_rate"))
    .withColumn("reference_created_at", F.current_timestamp())
)

ref_fx_legacy_final = (
    fx_long_legacy
    .groupBy(
        "year",
        "period_value",
        "time_frame",
        "currency_base",
        "currency_target",
    )
    .agg(F.avg("exchange_rate").alias("exchange_rate"))
    .withColumn("reference_created_at", F.current_timestamp())
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Data Quality
# MAGIC

# COMMAND ----------

def validate_fx_reference(
    df,
    key_columns,
    reference_name,
):
    row_count = df.count()

    if row_count == 0:
        raise ValueError(f"{reference_name} is empty.")

    invalid_rates = df.filter(
        F.col("exchange_rate").isNull()
        | (F.col("exchange_rate") <= 0)
    ).count()

    if invalid_rates > 0:
        raise ValueError(
            f"{reference_name} contains "
            f"{invalid_rates:,} invalid exchange rates."
        )

    duplicate_keys = (
        df
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_count = duplicate_keys.count()

    if duplicate_count > 0:
        display(duplicate_keys)
        raise ValueError(
            f"{reference_name} contains "
            f"{duplicate_count:,} duplicated business keys."
        )

    print(
        f"✓ {reference_name}: "
        f"{row_count:,} rows, positive rates, unique key."
    )


validate_fx_reference(
    ref_fx_normalized_final,
    [
        "year",
        "period_value",
        "time_frame",
        "currency_base",
        "currency_target",
        "country_name",
    ],
    "ref_fx_normalized",
)

validate_fx_reference(
    ref_fx_reporting_final,
    [
        "year",
        "period_value",
        "time_frame",
        "currency_base",
        "currency_target",
    ],
    "ref_fx_reporting",
)

validate_fx_reference(
    ref_fx_legacy_final,
    [
        "year",
        "period_value",
        "time_frame",
        "currency_base",
        "currency_target",
    ],
    "ref_fx_legacy",
)


# COMMAND ----------

display(
    ref_fx_normalized_final
    .orderBy(
        "year",
        "time_frame",
        "period_value",
        "currency_base",
        "currency_target",
        "country_name",
    )
    .limit(100)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Scrittura delle reference Silver
# MAGIC

# COMMAND ----------

references_to_write = [
    (ref_fx_normalized_final, TARGET_REF_FX_NORMALIZED),
    (ref_fx_reporting_final, TARGET_REF_FX_REPORTING),
    (ref_fx_legacy_final, TARGET_REF_FX_LEGACY),
]

for dataframe, target_table in references_to_write:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"✓ Created table: {target_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Verifica post-scrittura
# MAGIC

# COMMAND ----------

for expected_df, target_table in references_to_write:
    expected_count = expected_df.count()
    written_count = spark.table(target_table).count()

    print(
        f"{target_table} — "
        f"expected: {expected_count:,}; "
        f"written: {written_count:,}"
    )

    if expected_count != written_count:
        raise ValueError(
            f"Written row count differs from expected for {target_table}."
        )

print("✓ FX normalization completed successfully.")
