# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import reduce as _reduce

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

SOURCE_HARMONIZED          = f"{CATALOG}.{SILVER_SCHEMA}.harmonized"
TARGET_PANEL_VALORIZED     = f"{CATALOG}.{SILVER_SCHEMA}.panel_valorized"
REF_UTILITY_CONSOLIDAMENTI = f"{CATALOG}.{UTILS_SCHEMA}.utility_consolidamenti"

print(f"Source : {SOURCE_HARMONIZED}")
print(f"Target : {TARGET_PANEL_VALORIZED}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sell-In Valorization
# MAGIC
# MAGIC Reads `silver.harmonized` (fully enriched by `03_harmonize`), filters **Sell-In** records
# MAGIC (channel `Wholesale` / `Fabbricante`), valorizes them, and writes the unified output to
# MAGIC `silver.panel_valorized` — which also carries **Sell-Out pass-through** records (null
# MAGIC valorization columns) for the next notebook (`05_valorize_sellout`).
# MAGIC
# MAGIC ```
# MAGIC silver.harmonized
# MAGIC       │
# MAGIC       ├── channel IN (Wholesale, Fabbricante) ──────────────────────────────────────────┐
# MAGIC       │                                                                                 │
# MAGIC       │   1 — Processing by company type                                                │
# MAGIC       │     ├── Manufacturer / Transparence                                             │
# MAGIC       │     │     is_no_media flag                                                      │
# MAGIC       │     │     Homogeneous unit price  (key: company+item+country+currency+year+sem) │
# MAGIC       │     │     Geographic fallback  (Spain ↔ Iberia ↔ Portugal ↔ Andorra ↔ Italy)    │
# MAGIC       │     │     Temporal fallback  (previous semesters, most-recent-first)            │
# MAGIC       │     │     Special families  (SP_*, NA_*, NM_*, *_99 …) → pass-through own PM    │
# MAGIC       │     └── Dealer                                                                  │
# MAGIC       │           ├── Dealer Retail  → average retail price (key: sellout_dm)           │
# MAGIC       │           └── Dealer Wholesale  → skip price calculation                        │
# MAGIC       │                                                                                 │
# MAGIC       │   markup = avg_retail_price / pm_omogeneo  (key: sellout_dm)                    │
# MAGIC       │   Valorization:                                                                 │
# MAGIC       │     Manufacturer       → qty × pm_omogeneo                                      │
# MAGIC       │     Dealer Retail      → qty × avg_retail_price                                 │
# MAGIC       │     Dealer Wholesale   → qty × pm_omogeneo × markup                             │
# MAGIC       │                                                                                 │
# MAGIC       │   2 — Panel Overlap Check   →  consolidare_panel (boolean)                      │
# MAGIC       │   3 — Period Split          →  Mfr: 78.26/21.74 sem · 50/50 qtr                 │
# MAGIC       │                                                                                 │
# MAGIC       ├── channel NOT IN (Wholesale, Fabbricante) ──────────────────────────────────────┤
# MAGIC       │   Sell-Out pass-through (valorization columns = null → 05_valorize_sellout)     │
# MAGIC       │                                                                                 │
# MAGIC       └─────────────────────────────────────────────────────────────────────────────────┘
# MAGIC                                         ▼
# MAGIC                              silver.panel_valorized
# MAGIC ```
# MAGIC
# MAGIC **Output fields added by this notebook (null for Sell-Out pass-through):**
# MAGIC
# MAGIC | Field | Type | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `is_no_media` | int | 0=ok · 1=qty/amount≤0 · 2=special DM family · 3=PM missing after fallbacks |
# MAGIC | `pm_omogeneo` | double | Homogeneous unit price (sell-in side) |
# MAGIC | `avg_retail_price` | double | Average retail price by sellout_dm |
# MAGIC | `markup` | double | `avg_retail_price / pm_omogeneo` |
# MAGIC | `amount_homogeneous` | double | Valorized amount at sell-in price |
# MAGIC | `amount_sellout` | double | Valorized amount at sell-out price |
# MAGIC | `consolidare_panel` | boolean | Overlap between sell-in and sell-out panel |
# MAGIC | `_original_period` | string | Source period before period split (e.g. `2025_H2`) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Processing by company type
# MAGIC
# MAGIC ### Load and segmentation
# MAGIC
# MAGIC Loads `silver.harmonized` and splits records into three segments based on `company_type`
# MAGIC and `channel`. Sell-Out records are kept separate as a pass-through.

# COMMAND ----------

all_raw = spark.table(SOURCE_HARMONIZED)

# Sell-In = records that the company reported as sent to a dealer / manufacturer
sellin_raw = (
    all_raw
    .filter(F.col("channel").isin("Wholesale", "Fabbricante"))
    .withColumn("_year",     F.year(F.col("date")))
    .withColumn("_semester", F.when(F.month(F.col("date")) <= 6, F.lit("H1")).otherwise(F.lit("H2")))
    .withColumn("_dest_key", F.upper(F.trim(F.col("destination_country"))))
)

# Sell-Out = records where the company sold directly to end customers (dentists)
sellout_raw = all_raw.filter(~F.col("channel").isin("Wholesale", "Fabbricante"))

# Bifurcation within Sell-In: company type determines valorization path
manufacturer_df     = sellin_raw.filter(F.col("company_type").isin("manufacturer", "transparence"))
dealer_retail_df    = sellin_raw.filter((F.col("company_type") == "dealer") & (F.col("channel") == "Retail"))
dealer_wholesale_df = sellin_raw.filter(
    (F.col("company_type") == "dealer") & (F.col("channel").isin("Wholesale", "Fabbricante"))
)

mfr_cnt = manufacturer_df.count()
dr_cnt  = dealer_retail_df.count()
dw_cnt  = dealer_wholesale_df.count()
so_cnt  = sellout_raw.count()
print(f"Manufacturer / Transparence : {mfr_cnt:>10,}")
print(f"Dealer Retail               : {dr_cnt:>10,}")
print(f"Dealer Wholesale            : {dw_cnt:>10,}")
print(f"Sell-Out (pass-through)     : {so_cnt:>10,}")
print(f"Total                       : {mfr_cnt + dr_cnt + dw_cnt + so_cnt:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### is_no_media — anomaly flags (Manufacturer side)
# MAGIC
# MAGIC Flag applied to Manufacturer / Transparence records before valorization. Marks records that
# MAGIC cannot be used to compute a meaningful PM Omogeneo, or that require special treatment. Only
# MAGIC records with `is_no_media == 0` feed into the PM Omogeneo aggregation.
# MAGIC
# MAGIC | Value | When | What happens |
# MAGIC |-------|------|--------------|
# MAGIC | `0` | Normal record | Included in PM Omogeneo computation |
# MAGIC | `1` | `quantity ≤ 0` OR `fx_transaction_lc ≤ 0` | Returns / zero-qty lines — excluded from PM; `amount_homogeneous = 0` |
# MAGIC | `2` | `sellout_dm` matches a special family (`SP_*`, `NA_*`, `NM_*`, `*_99` …) | Promo / service items — record's own unit price used as-is |
# MAGIC | `3` | PM still null after all geographic + temporal fallbacks | No reference price available — `amount_homogeneous = null`, requires review |

# COMMAND ----------

_SPECIAL_DM_PATTERNS = [
    "SP_%", "NM_%", "NA_%", "%_99",
    "95_%", "94_%", "92_%", "91_%", "14_%",
    "%_98_%", "%_97_%", "%_96_%",
]

def _is_special_dm(col_name):
    cond = F.lit(False)
    for pat in _SPECIAL_DM_PATTERNS:
        cond = cond | F.col(col_name).like(pat)
    return cond

manufacturer_df = (
    manufacturer_df
    .withColumn(
        "is_no_media",
        F.when(
            (F.coalesce(F.col("quantity"),          F.lit(0.0)) <= 0) |
            (F.coalesce(F.col("fx_transaction_lc"), F.lit(0.0)) <= 0),
            F.lit(1),
        )
        .when(
            F.col("sellout_dm").isNotNull() & _is_special_dm("sellout_dm"),
            F.lit(2),
        )
        .otherwise(F.lit(0))
        .cast(IntegerType()),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Homogeneous Unit Price (PM Omogeneo)
# MAGIC
# MAGIC Consolidated average unit price per semester, computed only on records with `is_no_media == 0`.
# MAGIC
# MAGIC ```
# MAGIC PM key      = company + item_code + destination_country + billing_currency + year + semester
# MAGIC pm_omogeneo = SUM(fx_transaction_lc) / SUM(quantity)
# MAGIC ```

# COMMAND ----------

pm_df = (
    manufacturer_df
    .filter(F.col("is_no_media") == 0)
    .groupBy("company", "item_code", "_dest_key", "billing_currency", "_year", "_semester")
    .agg(
        F.sum("fx_transaction_lc").alias("_sum_amount"),
        F.sum("quantity").alias("_sum_qty"),
    )
    .withColumn(
        "pm_omogeneo",
        F.when(F.col("_sum_qty") == 0, F.lit(0.0))
         .otherwise(F.col("_sum_amount") / F.col("_sum_qty"))
         .cast(DoubleType()),
    )
    .filter(F.col("pm_omogeneo") > 0)
    .drop("_sum_amount", "_sum_qty")
)

_pm_join_cond = [
    F.col("m.company")          == F.col("pm.company"),
    F.col("m.item_code")        == F.col("pm.item_code"),
    F.col("m._dest_key")        == F.col("pm._dest_key"),
    F.col("m.billing_currency") == F.col("pm.billing_currency"),
    F.col("m._year")            == F.col("pm._year"),
    F.col("m._semester")        == F.col("pm._semester"),
]

_mfr_cols_no_pm = [c for c in manufacturer_df.columns if c != "pm_omogeneo"]

manufacturer_df = (
    manufacturer_df.alias("m")
    .join(pm_df.alias("pm"), _pm_join_cond, "left")
    .select(
        *[F.col(f"m.{c}") for c in _mfr_cols_no_pm],
        F.col("pm.pm_omogeneo"),
    )
)
print(f"PM entries computed: {pm_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geographic fallbacks
# MAGIC
# MAGIC For records where `pm_omogeneo` is still null after the primary join, try substituting the
# MAGIC destination country using the geographic equivalence chain:
# MAGIC `Spain ↔ Iberia ↔ Portugal ↔ Andorra ↔ Italy`

# COMMAND ----------

_GEO_FALLBACKS = [
    ("SPAIN",    "IBERIA"),   ("SPAIN",    "PORTUGAL"), ("SPAIN",    "ANDORRA"),
    ("IBERIA",   "SPAIN"),    ("PORTUGAL", "SPAIN"),    ("ANDORRA",  "SPAIN"),
    ("SPAIN",    "ITALY"),    ("ITALY",    "SPAIN"),
    ("PORTUGAL", "ITALY"),    ("ITALY",    "PORTUGAL"),
]

geo_pm_parts = []
for (from_c, to_c) in _GEO_FALLBACKS:
    geo_pm_parts.append(
        pm_df
        .filter(F.col("_dest_key") == to_c)
        .withColumn("_dest_key", F.lit(from_c))
    )

if geo_pm_parts:
    geo_pm_df = _reduce(lambda a, b: a.unionByName(b), geo_pm_parts)

    _geo_join_cond = [
        F.col("m.company")          == F.col("g.company"),
        F.col("m.item_code")        == F.col("g.item_code"),
        F.col("m._dest_key")        == F.col("g._dest_key"),
        F.col("m.billing_currency") == F.col("g.billing_currency"),
        F.col("m._year")            == F.col("g._year"),
        F.col("m._semester")        == F.col("g._semester"),
    ]

    _mfr_cols_no_pm = [c for c in manufacturer_df.columns if c != "pm_omogeneo"]

    manufacturer_df = (
        manufacturer_df.alias("m")
        .join(geo_pm_df.alias("g"), _geo_join_cond, "left")
        .select(
            *[F.col(f"m.{c}") for c in _mfr_cols_no_pm],
            F.when(
                (F.col("m.is_no_media") == 0) & F.col("m.pm_omogeneo").isNull(),
                F.col("g.pm_omogeneo"),
            ).otherwise(F.col("m.pm_omogeneo")).alias("pm_omogeneo"),
        )
    )

missing_geo = manufacturer_df.filter(F.col("pm_omogeneo").isNull() & (F.col("is_no_media") == 0)).count()
print(f"Missing PM after geo fallback: {missing_geo:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporal fallbacks
# MAGIC
# MAGIC For records still missing a PM after geographic fallbacks, try the most recent available
# MAGIC semester for the same `(company, item_code, destination_country, billing_currency)` key.
# MAGIC Records with no PM after all fallbacks are flagged `is_no_media = 3`.

# COMMAND ----------

_temporal_window = (
    Window
    .partitionBy("company", "item_code", "_dest_key", "billing_currency")
    .orderBy(F.desc("_year"), F.desc("_semester"))
)

pm_temporal = (
    pm_df
    .withColumn("_rn", F.row_number().over(_temporal_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_year", "_semester")
    .withColumnRenamed("pm_omogeneo", "_pm_fallback")
)

_tf_join_cond = [
    F.col("m.company")          == F.col("tf.company"),
    F.col("m.item_code")        == F.col("tf.item_code"),
    F.col("m._dest_key")        == F.col("tf._dest_key"),
    F.col("m.billing_currency") == F.col("tf.billing_currency"),
]

_mfr_cols_no_pm = [c for c in manufacturer_df.columns if c != "pm_omogeneo"]

manufacturer_df = (
    manufacturer_df.alias("m")
    .join(pm_temporal.alias("tf"), _tf_join_cond, "left")
    .select(
        *[F.col(f"m.{c}") for c in _mfr_cols_no_pm],
        F.coalesce(F.col("m.pm_omogeneo"), F.col("tf._pm_fallback")).alias("pm_omogeneo"),
    )
    .withColumn(
        "is_no_media",
        F.when(
            (F.col("is_no_media") == 0) & F.col("pm_omogeneo").isNull(),
            F.lit(3),
        ).otherwise(F.col("is_no_media")),
    )
)

missing_final = manufacturer_df.filter(F.col("is_no_media") == 3).count()
print(f"Records with is_no_media=3 (PM missing after all fallbacks): {missing_final:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Special families (is_no_media = 2) — pass-through
# MAGIC
# MAGIC For special DM families the aggregated PM is not meaningful. The record's own unit price
# MAGIC (`fx_transaction_lc / quantity`) is used as `pm_omogeneo` so that `amount_homogeneous`
# MAGIC reflects the original revenue.

# COMMAND ----------

manufacturer_df = (
    manufacturer_df
    .withColumn(
        "pm_omogeneo",
        F.when(
            (F.col("is_no_media") == 2)
            & F.col("quantity").isNotNull() & (F.col("quantity") != 0)
            & F.col("fx_transaction_lc").isNotNull(),
            (F.col("fx_transaction_lc") / F.col("quantity")).cast(DoubleType()),
        ).otherwise(F.col("pm_omogeneo")),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average retail prices (Dealer Retail)
# MAGIC
# MAGIC Computed from `dealer_retail_df` records aggregated by `sellout_dm`. Used as the basis for
# MAGIC the **markup** and to value both Dealer Retail and Dealer Wholesale records.

# COMMAND ----------

avg_retail_df = (
    dealer_retail_df
    .filter(
        F.col("sellout_dm").isNotNull()
        & F.col("fx_transaction_lc").isNotNull() & (F.col("fx_transaction_lc") > 0)
        & F.col("quantity").isNotNull() & (F.col("quantity") > 0)
    )
    .groupBy("sellout_dm", "_year", "_semester")
    .agg(
        (F.sum("fx_transaction_lc") / F.sum("quantity")).alias("avg_retail_price"),
    )
    .filter(F.col("avg_retail_price") > 0)
)
print(f"Retail price entries: {avg_retail_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retail Markup
# MAGIC
# MAGIC ```
# MAGIC markup = avg_retail_price / pm_omogeneo      (key: sellout_dm + year + semester)
# MAGIC ```
# MAGIC
# MAGIC Applied to Dealer Wholesale records to convert from sell-in PM to sell-out price.

# COMMAND ----------

pm_by_dm = (
    manufacturer_df
    .filter(F.col("is_no_media") == 0)
    .groupBy("sellout_dm", "_year", "_semester")
    .agg((F.sum("fx_transaction_lc") / F.sum("quantity")).alias("_pm_by_dm"))
    .filter(F.col("_pm_by_dm") > 0)
)

markup_df = (
    avg_retail_df.alias("r")
    .join(pm_by_dm.alias("p"), ["sellout_dm", "_year", "_semester"], "left")
    .withColumn(
        "markup",
        F.when(
            F.col("p._pm_by_dm").isNotNull() & (F.col("p._pm_by_dm") > 0),
            F.col("r.avg_retail_price") / F.col("p._pm_by_dm"),
        ).otherwise(F.lit(None).cast(DoubleType())),
    )
    .select("sellout_dm", "_year", "_semester", "avg_retail_price", "markup")
)
print(f"Markup entries: {markup_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valorization
# MAGIC
# MAGIC Applies `pm_omogeneo`, `avg_retail_price` and `markup` to the three segments and unions
# MAGIC them into a single `sellin_df`.
# MAGIC
# MAGIC | Channel class | `amount_homogeneous` (sell-in) | `amount_sellout` (sell-out) |
# MAGIC |---|---|---|
# MAGIC | Manufacturer | `quantity × pm_omogeneo` | = `amount_homogeneous` |
# MAGIC | Dealer Retail | `quantity × avg_retail_price` | = `amount_homogeneous` |
# MAGIC | Dealer Wholesale | `quantity × pm_omogeneo × markup` | = `amount_homogeneous` |

# COMMAND ----------

def _join_markup(df):
    return (
        df.alias("d")
        .join(
            markup_df.alias("mk"),
            (F.col("d.sellout_dm") == F.col("mk.sellout_dm")) &
            (F.col("d._year")      == F.col("mk._year")) &
            (F.col("d._semester")  == F.col("mk._semester")),
            "left",
        )
        .select("d.*", F.col("mk.avg_retail_price"), F.col("mk.markup"))
    )

def _add_null_pricing(df):
    return (
        df
        .withColumn("is_no_media",      F.lit(0).cast(IntegerType()))
        .withColumn("pm_omogeneo",      F.lit(None).cast(DoubleType()))
        .withColumn("avg_retail_price", F.lit(None).cast(DoubleType()))
        .withColumn("markup",           F.lit(None).cast(DoubleType()))
    )

# Manufacturer: amount_homogeneous = qty × pm_omogeneo
manufacturer_valued = (
    manufacturer_df
    .transform(_join_markup)
    .withColumn(
        "amount_homogeneous",
        F.when(F.col("is_no_media") == 1, F.lit(0.0))
         .when(F.col("is_no_media") == 2, F.col("fx_transaction_lc"))
         .when(F.col("is_no_media") == 3, F.lit(None).cast(DoubleType()))
         .when(F.col("pm_omogeneo").isNotNull() & F.col("quantity").isNotNull(),
               F.col("quantity") * F.col("pm_omogeneo"))
         .otherwise(F.lit(None).cast(DoubleType()))
         .cast(DoubleType()),
    )
    .withColumn("amount_sellout", F.col("amount_homogeneous"))
)

# Dealer Retail: amount_homogeneous = qty × avg_retail_price
dealer_retail_valued = (
    dealer_retail_df
    .transform(_add_null_pricing)
    .transform(_join_markup)
    .withColumn(
        "amount_homogeneous",
        F.when(F.col("avg_retail_price").isNotNull() & F.col("quantity").isNotNull(),
               F.col("quantity") * F.col("avg_retail_price"))
         .otherwise(F.lit(None).cast(DoubleType()))
         .cast(DoubleType()),
    )
    .withColumn("amount_sellout", F.col("amount_homogeneous"))
)

# Dealer Wholesale: amount_homogeneous = qty × (avg_retail_price / markup) = qty × pm_omogeneo
#                   amount_sellout     = qty × avg_retail_price
dealer_wholesale_valued = (
    dealer_wholesale_df
    .transform(_add_null_pricing)
    .transform(_join_markup)
    .withColumn(
        "pm_omogeneo",
        F.when(
            F.col("avg_retail_price").isNotNull() & F.col("markup").isNotNull() & (F.col("markup") > 0),
            F.col("avg_retail_price") / F.col("markup"),
        ).otherwise(F.lit(None).cast(DoubleType())),
    )
    .withColumn(
        "amount_homogeneous",
        F.when(F.col("pm_omogeneo").isNotNull() & F.col("quantity").isNotNull(),
               F.col("quantity") * F.col("pm_omogeneo"))
         .otherwise(F.lit(None).cast(DoubleType()))
         .cast(DoubleType()),
    )
    .withColumn(
        "amount_sellout",
        F.when(F.col("amount_homogeneous").isNotNull() & F.col("markup").isNotNull(),
               F.col("amount_homogeneous") * F.col("markup"))
         .otherwise(F.lit(None).cast(DoubleType()))
         .cast(DoubleType()),
    )
)

sellin_df = _reduce(
    lambda a, b: a.unionByName(b),
    [manufacturer_valued, dealer_retail_valued, dealer_wholesale_valued],
)
print(f"Total records after valorization: {sellin_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Panel Overlap Check
# MAGIC
# MAGIC Determines whether each record's company/customer/channel appears in both the sell-in and
# MAGIC sell-out panels. The `consolidare_panel` flag prevents double-counting downstream.
# MAGIC
# MAGIC Join key: `company + customer + channel` → `consolidare_panel` (boolean).

# COMMAND ----------

if spark.catalog.tableExists(REF_UTILITY_CONSOLIDAMENTI):
    consolidamenti = (
        spark.table(REF_UTILITY_CONSOLIDAMENTI)
        .select(
            F.upper(F.trim(F.col("company"))).alias("_cons_company"),
            F.upper(F.trim(F.col("customer"))).alias("_cons_customer"),
            F.upper(F.trim(F.col("channel_class"))).alias("_cons_channel"),
            F.col("consolidare_panel").cast("boolean"),
        )
        .dropDuplicates(["_cons_company", "_cons_customer", "_cons_channel"])
    )

    sellin_df = (
        sellin_df.alias("s")
        .join(
            consolidamenti.alias("c"),
            (F.upper(F.trim(F.col("s.company")))  == F.col("c._cons_company")) &
            (F.upper(F.trim(F.col("s.customer"))) == F.col("c._cons_customer")) &
            (F.upper(F.trim(F.col("s.channel")))  == F.col("c._cons_channel")),
            "left",
        )
        .select("s.*", F.col("c.consolidare_panel"))
    )
    print(f"✓ Consolidamenti joined from {REF_UTILITY_CONSOLIDAMENTI}")
else:
    sellin_df = sellin_df.withColumn("consolidare_panel", F.lit(None).cast("boolean"))
    print(f"⚠️  {REF_UTILITY_CONSOLIDAMENTI} not found — consolidare_panel set to null")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Period Split
# MAGIC
# MAGIC Applies only to **Manufacturer / Transparence** records — Dealers pass through unchanged.
# MAGIC
# MAGIC | Company type | Granularity | Split |
# MAGIC |---|---|---|
# MAGIC | Manufacturer / Transparence | Semester | 78.26% current period + 21.74% next period |
# MAGIC | Manufacturer / Transparence | Quarter | 50% current + 50% next quarter |
# MAGIC | Dealer (any) | any | no split — record passed as-is |
# MAGIC
# MAGIC `_original_period` (e.g. `2025_H2`) is written to every row and used as the MERGE key so
# MAGIC that re-running the same semester replaces only its own records.

# COMMAND ----------

_SEM_SPLIT_CURRENT = 0.7826
_SEM_SPLIT_NEXT    = 0.2174
_QTR_SPLIT_CURRENT = 0.5
_QTR_SPLIT_NEXT    = 0.5

sellin_with_period = (
    sellin_df
    .withColumn("_year",         F.year(F.col("date")))
    .withColumn("_semester",     F.when(F.month(F.col("date")) <= 6, F.lit("H1")).otherwise(F.lit("H2")))
    .withColumn("_is_quarterly", F.col("time_frames").isin("quarterly", "trimestrale"))
    .withColumn("_original_period",
        F.concat(F.col("_year").cast("string"), F.lit("_"), F.col("_semester")))
)

_is_mfr = F.col("company_type").isin("manufacturer", "transparence")

# Period 1 — current (scaled fraction)
mfr_p1 = (
    sellin_with_period.filter(_is_mfr)
    .withColumn("_pct",
        F.when(F.col("_is_quarterly"), F.lit(_QTR_SPLIT_CURRENT))
         .otherwise(F.lit(_SEM_SPLIT_CURRENT)))
    .withColumn("quantity",           F.col("quantity")           * F.col("_pct"))
    .withColumn("amount_homogeneous", F.col("amount_homogeneous") * F.col("_pct"))
    .withColumn("amount_sellout",     F.col("amount_sellout")     * F.col("_pct"))
    .drop("_pct")
)

# Period 2 — next period (remainder), date shifted forward 6 months (semester) or 3 months (quarter)
mfr_p2 = (
    sellin_with_period.filter(_is_mfr)
    .withColumn("_pct",
        F.when(F.col("_is_quarterly"), F.lit(_QTR_SPLIT_NEXT))
         .otherwise(F.lit(_SEM_SPLIT_NEXT)))
    .withColumn("quantity",           F.col("quantity")           * F.col("_pct"))
    .withColumn("amount_homogeneous", F.col("amount_homogeneous") * F.col("_pct"))
    .withColumn("amount_sellout",     F.col("amount_sellout")     * F.col("_pct"))
    .withColumn("date",
        F.when(F.col("_is_quarterly"),
               F.date_trunc("month", F.add_months(F.col("date"), 3)).cast("date"))
         .otherwise(
               F.date_trunc("month", F.add_months(F.col("date"), 6)).cast("date")))
    .drop("_pct")
)

# Dealer — pass-through, no split applied
dealer_pt = sellin_with_period.filter(~_is_mfr)

sellin_output = (
    _reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [mfr_p1, mfr_p2, dealer_pt],
    )
    .drop("_year", "_semester", "_is_quarterly", "_dest_key")
)

# Sell-Out pass-through — null valorization columns; values filled by 05_valorize_sellout
sellout_pt = (
    sellout_raw
    .withColumn("is_no_media",        F.lit(None).cast(IntegerType()))
    .withColumn("pm_omogeneo",        F.lit(None).cast(DoubleType()))
    .withColumn("avg_retail_price",   F.lit(None).cast(DoubleType()))
    .withColumn("markup",             F.lit(None).cast(DoubleType()))
    .withColumn("amount_homogeneous", F.lit(None).cast(DoubleType()))
    .withColumn("amount_sellout",     F.lit(None).cast(DoubleType()))
    .withColumn("consolidare_panel",  F.lit(None).cast("boolean"))
    .withColumn("_original_period",
        F.concat(
            F.year(F.col("date")).cast("string"),
            F.lit("_"),
            F.when(F.month(F.col("date")) <= 6, F.lit("H1")).otherwise(F.lit("H2")),
        ))
)

output_df = sellin_output.unionByName(sellout_pt, allowMissingColumns=True)

si_cnt = sellin_output.count()
so_cnt = sellout_pt.count()
print(f"Sell-In valorized     : {si_cnt:>10,}")
print(f"Sell-Out pass-through : {so_cnt:>10,}")
print(f"Total output          : {si_cnt + so_cnt:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to `silver.panel_valorized`
# MAGIC
# MAGIC Delta MERGE insert-only. MERGE key: `(company, item_code, date, customer, channel, _original_period)`.
# MAGIC
# MAGIC Re-running the same semester overwrites only that semester's records via `_original_period`.

# COMMAND ----------

if spark.catalog.tableExists(TARGET_PANEL_VALORIZED):
    (
        DeltaTable.forName(spark, TARGET_PANEL_VALORIZED)
        .alias("t")
        .merge(
            output_df.alias("s"),
            """
            t.company              = s.company
            AND t.item_code        = s.item_code
            AND t.date             = s.date
            AND t.customer         = s.customer
            AND t.channel          = s.channel
            AND t._original_period = s._original_period
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        output_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_PANEL_VALORIZED)
    )

print(f"✓ Written to {TARGET_PANEL_VALORIZED}: {output_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality

# COMMAND ----------

# Coverage by company_type and company
(
    sellin_df
    .groupBy("company_type", "company")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("pm_omogeneo").isNotNull(),        1).otherwise(0)).alias("with_pm"),
        F.sum(F.when(F.col("amount_homogeneous").isNotNull(),  1).otherwise(0)).alias("valorized"),
        F.sum(F.when(F.col("amount_sellout").isNotNull(),      1).otherwise(0)).alias("with_sellout"),
    )
    .withColumn("pm_pct", F.round(F.col("with_pm") / F.col("total") * 100, 1))
    .orderBy("company_type", "company")
    .display()
)

# COMMAND ----------

# is_no_media distribution
sellin_df.groupBy("is_no_media").count().orderBy("is_no_media").display()

# COMMAND ----------

# Item codes missing PM (is_no_media=3) — for manual review / escalation
missing_pm = (
    sellin_df
    .filter(F.col("is_no_media") == 3)
    .groupBy("company", "item_code", "sellout_dm", "destination_country", "billing_currency")
    .agg(F.count("*").alias("records"), F.sum("quantity").alias("total_qty"))
    .orderBy(F.desc("records"))
)
print(f"Distinct combos missing PM: {missing_pm.count():,}")
display(missing_pm)