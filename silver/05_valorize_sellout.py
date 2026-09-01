# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

SOURCE_PANEL_VALORIZED     = f"{CATALOG}.{SILVER_SCHEMA}.panel_valorized"
TARGET_SELLOUT_VALORIZED   = f"{CATALOG}.{SILVER_SCHEMA}.sellout_valorized"
REF_UTILITY_CONSOLIDAMENTI = f"{CATALOG}.{UTILS_SCHEMA}.utility_consolidamenti"

print(f"Source : {SOURCE_PANEL_VALORIZED}")
print(f"Target : {TARGET_SELLOUT_VALORIZED}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sell-Out Valorization
# MAGIC
# MAGIC Reads `silver.panel_valorized`, filters the **Sell-Out pass-through** records (those written
# MAGIC by `04_valorize_sellin` with null valorization columns), applies the sell-out valorization
# MAGIC logic, and writes the result to `silver.sellout_valorized`.
# MAGIC
# MAGIC ```
# MAGIC silver.panel_valorized
# MAGIC       │
# MAGIC       └── is_no_media IS NULL  (sell-out pass-through records)
# MAGIC             │
# MAGIC             │  1 — is_no_media flagging
# MAGIC             │        0 = normal record
# MAGIC             │        1 = quantity ≤ 0  OR  fx_transaction_lc ≤ 0  (return / zero-line)
# MAGIC             │        2 = sellout_dm matches a special family (promo / service item)
# MAGIC             │
# MAGIC             │  2 — Valorization
# MAGIC             │        is_no_media = 0  →  amount_sellout = fx_transaction_lc
# MAGIC             │        is_no_media = 1  →  amount_sellout = 0
# MAGIC             │        is_no_media = 2  →  amount_sellout = fx_transaction_lc (own revenue)
# MAGIC             │        is_no_media = 3  →  amount_sellout = null  (review required)
# MAGIC             │
# MAGIC             │  3 — Join PM Omogeneo from sell-in side
# MAGIC             │        key: company + item_code + destination_country + billing_currency
# MAGIC             │        → populates pm_omogeneo for cross-reference with sell-in panel
# MAGIC             │
# MAGIC             │  4 — Panel Overlap Check
# MAGIC             │        JOIN utility_consolidamenti → consolidare_panel (boolean)
# MAGIC             │
# MAGIC             ▼
# MAGIC       silver.sellout_valorized
# MAGIC ```
# MAGIC
# MAGIC **Output fields filled by this notebook (were null in `panel_valorized`):**
# MAGIC
# MAGIC | Field | Type | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `is_no_media` | int | 0=ok · 1=qty/amount≤0 · 2=special DM family · 3=PM missing |
# MAGIC | `pm_omogeneo` | double | Reference sell-in price (from 04 PM table, for cross-reference) |
# MAGIC | `amount_sellout` | double | Valorized sell-out amount in local currency |
# MAGIC | `consolidare_panel` | boolean | Overlap between sell-in and sell-out panel |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load sell-out pass-through records
# MAGIC
# MAGIC Sell-Out pass-through records are identified by `is_no_media IS NULL`, written by
# MAGIC `04_valorize_sellin` for all `channel NOT IN ('Wholesale', 'Fabbricante')` rows.

# COMMAND ----------

panel = spark.table(SOURCE_PANEL_VALORIZED)

# Sell-Out pass-through: valorization columns are null (set by 04_valorize_sellin)
sellout_df = panel.filter(F.col("is_no_media").isNull())

# Already-valorized sell-in records (carry over to output unchanged)
sellin_done = panel.filter(F.col("is_no_media").isNotNull())

so_cnt = sellout_df.count()
si_cnt = sellin_done.count()
print(f"Sell-Out pass-through   : {so_cnt:>10,}")
print(f"Sell-In already valued  : {si_cnt:>10,}")
print(f"Total in panel_valorized: {so_cnt + si_cnt:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. is_no_media — anomaly flags
# MAGIC
# MAGIC Same flag logic as the sell-in side. Applied here to sell-out records before valorization.
# MAGIC
# MAGIC | Value | When | What happens |
# MAGIC |-------|------|--------------|
# MAGIC | `0` | Normal record | `amount_sellout = fx_transaction_lc` |
# MAGIC | `1` | `quantity ≤ 0` OR `fx_transaction_lc ≤ 0` | Returns / zero-qty lines — `amount_sellout = 0` |
# MAGIC | `2` | `sellout_dm` matches a special family | Promo / service items — `amount_sellout = fx_transaction_lc` (own revenue) |
# MAGIC | `3` | `fx_transaction_lc` is null | No FX-converted revenue — `amount_sellout = null`, requires review |

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

sellout_df = (
    sellout_df
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
        .when(
            F.col("fx_transaction_lc").isNull(),
            F.lit(3),
        )
        .otherwise(F.lit(0))
        .cast(IntegerType()),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Valorization
# MAGIC
# MAGIC For sell-out records, the revenue is already in `fx_transaction_lc` (the actual amount the
# MAGIC company received from the end customer in local currency). No price lookup is required —
# MAGIC `amount_sellout` is set directly from `fx_transaction_lc`.

# COMMAND ----------

sellout_df = (
    sellout_df
    .withColumn(
        "amount_sellout",
        F.when(F.col("is_no_media") == 1, F.lit(0.0).cast(DoubleType()))
         .when(F.col("is_no_media") == 3, F.lit(None).cast(DoubleType()))
         .when(F.col("fx_transaction_lc").isNotNull(), F.col("fx_transaction_lc"))
         .otherwise(F.lit(None).cast(DoubleType()))
         .cast(DoubleType()),
    )
    .withColumn("amount_homogeneous", F.lit(None).cast(DoubleType()))  # not applicable to sell-out
    .withColumn("avg_retail_price",   F.lit(None).cast(DoubleType()))
    .withColumn("markup",             F.lit(None).cast(DoubleType()))
)

print(f"Valorized records: {sellout_df.count():,}")
sellout_df.groupBy("is_no_media").count().orderBy("is_no_media").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reference PM from sell-in side
# MAGIC
# MAGIC Joins the PM Omogeneo computed in `04_valorize_sellin` back onto sell-out records.
# MAGIC Provides a sell-in reference price for cross-panel comparison — does not affect
# MAGIC `amount_sellout`.
# MAGIC
# MAGIC Key: `company + item_code + destination_country + billing_currency` (most recent period).

# COMMAND ----------

pm_reference = (
    sellin_done
    .filter(F.col("pm_omogeneo").isNotNull())
    .withColumn("_year",     F.year(F.col("date")))
    .withColumn("_semester", F.when(F.month(F.col("date")) <= 6, F.lit("H1")).otherwise(F.lit("H2")))
    .withColumn("_dest_key", F.upper(F.trim(F.col("destination_country"))))
    .groupBy("company", "item_code", "_dest_key", "billing_currency")
    .agg(F.first("pm_omogeneo", ignorenulls=True).alias("_pm_ref"))
    .filter(F.col("_pm_ref").isNotNull())
)

_pm_ref_join = [
    F.col("s.company")          == F.col("p.company"),
    F.col("s.item_code")        == F.col("p.item_code"),
    F.upper(F.trim(F.col("s.destination_country"))) == F.col("p._dest_key"),
    F.col("s.billing_currency") == F.col("p.billing_currency"),
]

_so_cols_no_pm = [c for c in sellout_df.columns if c != "pm_omogeneo"]

sellout_df = (
    sellout_df.alias("s")
    .join(pm_reference.alias("p"), _pm_ref_join, "left")
    .select(
        *[F.col(f"s.{c}") for c in _so_cols_no_pm],
        F.col("p._pm_ref").alias("pm_omogeneo"),
    )
)

pm_joined = sellout_df.filter(F.col("pm_omogeneo").isNotNull()).count()
print(f"Sell-out records with reference PM: {pm_joined:,} / {sellout_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Panel Overlap Check
# MAGIC
# MAGIC Same logic as `04_valorize_sellin`: joins `utility_consolidamenti` to flag sell-out records
# MAGIC that appear in the sell-in panel. The `consolidare_panel` field is used downstream to avoid
# MAGIC double-counting during panel consolidation.

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

    sellout_df = (
        sellout_df.alias("s")
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
    sellout_df = sellout_df.withColumn("consolidare_panel", F.lit(None).cast("boolean"))
    print(f"⚠️  {REF_UTILITY_CONSOLIDAMENTI} not found — consolidare_panel set to null")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to `silver.sellout_valorized`
# MAGIC
# MAGIC Only the sell-out records (valorized in this notebook) are written. The already-valorized
# MAGIC sell-in records from `panel_valorized` are not duplicated here — they remain in
# MAGIC `panel_valorized` for any downstream Gold notebooks that need both sides together.
# MAGIC
# MAGIC MERGE key: `(company, item_code, date, customer, channel, _original_period)`.

# COMMAND ----------

if spark.catalog.tableExists(TARGET_SELLOUT_VALORIZED):
    (
        DeltaTable.forName(spark, TARGET_SELLOUT_VALORIZED)
        .alias("t")
        .merge(
            sellout_df.alias("s"),
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
        sellout_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_SELLOUT_VALORIZED)
    )

print(f"✓ Written to {TARGET_SELLOUT_VALORIZED}: {sellout_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality

# COMMAND ----------

# is_no_media distribution
sellout_df.groupBy("is_no_media").count().orderBy("is_no_media").display()

# COMMAND ----------

# Coverage by company
(
    sellout_df
    .groupBy("company_type", "company")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("amount_sellout").isNotNull(), 1).otherwise(0)).alias("valorized"),
        F.sum(F.when(F.col("pm_omogeneo").isNotNull(),   1).otherwise(0)).alias("with_pm_ref"),
        F.sum(F.when(F.col("is_no_media") == 3,          1).otherwise(0)).alias("missing_fx"),
    )
    .withColumn("coverage_pct", F.round(F.col("valorized") / F.col("total") * 100, 1))
    .orderBy("company_type", "company")
    .display()
)

# COMMAND ----------

# Records with no FX-converted revenue (is_no_media=3) — for review
(
    sellout_df
    .filter(F.col("is_no_media") == 3)
    .groupBy("company", "item_code", "destination_country", "billing_currency")
    .agg(F.count("*").alias("records"), F.sum("quantity").alias("total_qty"))
    .orderBy(F.desc("records"))
    .display()
)