# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

TARGET_DIM_COMPANIES = f"{CATALOG}.{SILVER_SCHEMA}.dim_companies"
TARGET_DIM_PRODUCTS  = f"{CATALOG}.{SILVER_SCHEMA}.dim_products"

print(f"Source catalog : {REFERENCE_CATALOG}.{REFERENCE_SCHEMA}")
print(f"dim_companies  : {TARGET_DIM_COMPANIES}")
print(f"dim_products   : {TARGET_DIM_PRODUCTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimension Snapshots
# MAGIC
# MAGIC This notebook snapshots volatile PDB dimensions into Silver Delta tables.
# MAGIC Run once per pipeline execution — write mode is OVERWRITE (full refresh).
# MAGIC
# MAGIC | Target | Source tables | Rationale |
# MAGIC |---|---|---|
# MAGIC | `silver.dim_companies` | companies + companies_relation | Changes when distributors are added or relationships change |
# MAGIC | `silver.dim_products`  | prods + prods_versions + master_code + mc_lv1/2/3 | Changes when new codes are added, descriptions updated |

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLORE — run once to inspect PDB schema, then fill in the SELECT cells below

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT manufacturer, dealer, count(*)
# MAGIC FROM reference_data.master_data.companies
# MAGIC GROUP BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM reference_data.master_data.companies

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE {REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.companies").display()

# COMMAND ----------

for level in ["mc_lvl1", "mc_lvl2", "mc_lvl3"]:
    print(f"\n--- {level} ---")
    spark.sql(f"DESCRIBE TABLE {REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.{level}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_companies

# COMMAND ----------

# MAGIC %md
# MAGIC | company_type   | Esempio                          | Ruolo nel processo                                   |
# MAGIC |----------------|----------------------------------|------------------------------------------------------|
# MAGIC | "manufacturer" | company_027, company_040, company_045, ...    | Produce il prodotto → PM Omogeneo calcolato da sell-in |
# MAGIC | "dealer"       | company_039, company_035, ...        | Distribuisce/rivende → valorizzazione via prezzi retail |
# MAGIC | "company"      | Studio dentistico, clinica, ...  | Cliente finale → non dovrebbe apparire come fonte di sell-out |

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sellout_portfolio.silver.dim_company

# COMMAND ----------

# ============================================================
# DIM COMPANY
# ============================================================
companies_df = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.companies")

dim_company_df = (
    companies_df
    .select(
        F.col("id"),
        normalize_company_name(F.col("company_name")).alias("name"),
        F.when(F.col("manufacturer"), F.lit("manufacturer"))
         .when(F.col("dealer"), F.lit("dealer"))
         .alias("type"),
        F.col("active"),
        F.col("data_creation").cast("date").alias("create_date"),
    )
)

(
    dim_company_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_company")
)

print("✓ dim_company creata con company normalizzata")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sellout_portfolio.silver.dim_company

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_products

# COMMAND ----------

dim_product_master_source_df = spark.sql(f"""
WITH extras AS (
    SELECT
        pea.id_prod,
        array_join(
            transform(
                array_sort(
                    collect_list(
                        named_struct(
                            'field_code', eaf.field_code,
                            'val',
                            COALESCE(
                                eav.val_label,
                                pea.val_text,
                                CAST(pea.val_decimal AS STRING),
                                CAST(pea.val_integer AS STRING),
                                CAST(pea.val_boolean AS STRING),
                                DATE_FORMAT(pea.val_date, 'yyyy-MM-dd'),
                                pea.source_raw_val
                            )
                        )
                    )
                ),
                x -> concat(x.field_code, '=', x.val)
            ),
            '; '
        ) AS extra
    FROM reference_data.master_data.prods_extra_attrib AS pea
    JOIN reference_data.master_data.prods_extra_attrib_fields AS eaf
        ON eaf.id = pea.id_prod_extra_attrib_field
    LEFT JOIN reference_data.master_data.prods_extra_attrib_allowed_val AS eav
        ON eav.id = pea.id_prod_extra_attrib_allowed_val
    GROUP BY pea.id_prod
)
SELECT
    split_part(p.company_item_code, '|', 1) AS company,
    split_part(p.company_item_code, '|', 2) AS item_code,
    pv.description,                           -- [18_ITEM DESCRIPTION_CLEANED]
    c.company_name AS customer,               -- [19_Company-Dealer]
    b.brand AS brand,                         -- [20_SELL-OUT BRAND]
    pc.prefix_code,                           -- [22_SELLOUT_PREFIX_CODE]
    fn.father_name,                           -- [23_FATHER NAME]
    CONCAT_WS('_',
        LPAD(CAST(mc1.code AS STRING), 2, '0'),
        LPAD(CAST(mc2.code AS STRING), 2, '0'),
        LPAD(CAST(mc3.code AS STRING), 2, '0')
    ) AS mc_code,                            -- [SELLOUT_DM]
    pack.pack,                               -- [26_PACKAGING]
    pq.inner_count,                          -- [26_CONTENT]
    pq.inner_qty,                            -- [26_CONTENT]
    pmu.measure_unit AS pack_measure_unit,   -- [26_UM MEASURE]
    feat.feature,                            -- [27_TYPE/FEATURE]
    meas.measure,                            -- [27_MEASURE]
    e.extra AS extra
FROM reference_data.master_data.prods_versions AS pv
JOIN reference_data.master_data.prods AS p
    ON p.id = pv.id_prod
LEFT JOIN reference_data.master_data.companies AS c
    ON c.id = pv.id_dealer
LEFT JOIN reference_data.master_data.brands AS b
    ON b.id = pv.id_brand
LEFT JOIN reference_data.master_data.prods_prefix_codes AS pc
    ON pc.id = pv.id_prefix_code
LEFT JOIN reference_data.master_data.prods_father_names AS fn
    ON fn.id = pv.id_father_name
LEFT JOIN reference_data.master_data.master_code AS mc
    ON mc.id = pv.id_mc
LEFT JOIN reference_data.master_data.mc_lvl1 AS mc1
    ON mc1.id = mc.id_mc_lvl1
LEFT JOIN reference_data.master_data.mc_lvl2 AS mc2
    ON mc2.id = mc.id_mc_lvl2
LEFT JOIN reference_data.master_data.mc_lvl3 AS mc3
    ON mc3.id = mc.id_mc_lvl3
LEFT JOIN reference_data.master_data.prods_pack AS pack
    ON pack.id = pv.id_pack
LEFT JOIN reference_data.master_data.prods_pack_qty AS pq
    ON pq.id_prod_version = pv.id
LEFT JOIN reference_data.master_data.prods_pack_measure_units AS pmu
    ON pmu.id = pq.id_pack_measure_unit
LEFT JOIN reference_data.master_data.prods_features AS feat
    ON feat.id = pv.id_feature
LEFT JOIN reference_data.master_data.prods_measures AS meas
    ON meas.id = pv.id_measure
LEFT JOIN extras AS e
    ON e.id_prod = p.id
""")

dim_product_master_df = dim_product_master_source_df.withColumn(
    "company",
    normalize_company_name(F.col("company")),
)

(
    dim_product_master_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_product_master")
)

print("✓ dim_product_master creata con company normalizzata")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sellout_portfolio.silver.dim_product_master limit 10

# COMMAND ----------

# prods_v = (
#     spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.prods_versions")
#     .filter(F.col("is_current") == True)
#     .select(
#         F.col("id_prod"),
#         F.col("version"),
#         F.col("description"),
#         F.col("id_mc"),
#         F.col("id_dealer"),
#         F.col("id_manufacturer"),
#         F.col("id_prefix_code"),
#         F.col("id_father_name"),
#         F.col("valid_from"),
#         F.col("valid_to"),
#     )
# )

# prods_base     = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.prods").select("id", "company_item_code")
# master_code    = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.master_code")
# mc_lvl1        = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.mc_lvl1")
# mc_lvl2        = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.mc_lvl2")
# mc_lvl3        = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.mc_lvl3")
# prefix_codes   = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.prods_prefix_codes")
# father_names   = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.prods_father_names")
# companies_slim = spark.table(f"{REFERENCE_CATALOG}.{REFERENCE_SCHEMA}.companies").select("id", "company_name")

# # master_code + gerarchia mc_lvl1/2/3
# mc_hierarchy = (
#     master_code
#     .join(
#         mc_lvl1.select(F.col("id").alias("id_mc_lvl1"), F.col("code").alias("mc_lvl1_code"), F.col("desc").alias("mc_lvl1_desc")),
#         on="id_mc_lvl1", how="left",
#     )
#     .join(
#         mc_lvl2.select(F.col("id").alias("id_mc_lvl2"), F.col("code").alias("mc_lvl2_code"), F.col("desc").alias("mc_lvl2_desc")),
#         on="id_mc_lvl2", how="left",
#     )
#     .join(
#         mc_lvl3.select(F.col("id").alias("id_mc_lvl3"), F.col("code").alias("mc_lvl3_code"), F.col("desc").alias("mc_lvl3_desc")),
#         on="id_mc_lvl3", how="left",
#     )
#     .select(
#         F.col("id").alias("master_code_id"),
#         "mc_lvl1_code", "mc_lvl1_desc",
#         "mc_lvl2_code", "mc_lvl2_desc",
#         "mc_lvl3_code", "mc_lvl3_desc",
#     )
# )

# _split = F.split(F.col("company_item_code"), r"\|")

# dim_products = (
#     prods_v
#     .join(
#         prods_base.select(F.col("id").alias("_prod_id"), "company_item_code"),
#         prods_v["id_prod"] == F.col("_prod_id"),
#         "left",
#     )
#     .join(
#         mc_hierarchy,
#         prods_v["id_mc"] == F.col("master_code_id"),
#         "left",
#     )
#     .join(
#         prefix_codes.select(F.col("id").alias("_id_prefix_code"), "prefix_code"),
#         prods_v["id_prefix_code"] == F.col("_id_prefix_code"),
#         "left",
#     )
#     .join(
#         father_names.select(F.col("id").alias("_id_father_name"), "father_name"),
#         prods_v["id_father_name"] == F.col("_id_father_name"),
#         "left",
#     )
#     .join(
#         companies_slim.select(F.col("id").alias("_id_dealer"), F.col("company_name").alias("dealer_name")),
#         prods_v["id_dealer"] == F.col("_id_dealer"),
#         "left",
#     )
#     .join(
#         companies_slim.select(F.col("id").alias("_id_manufacturer"), F.col("company_name").alias("manufacturer_name")),
#         prods_v["id_manufacturer"] == F.col("_id_manufacturer"),
#         "left",
#     )
#     .select(
#         F.col("_prod_id").alias("prod_id"),
#         _split.getItem(0).alias("company"),         
#         _split.getItem(1).alias("item_code"),        
#         F.col("description"),
#         F.col("prefix_code"),
#         F.col("father_name"),
#         F.col("dealer_name"),
#         F.col("manufacturer_name"),
#         F.col("mc_lvl1_code"), F.col("mc_lvl1_desc"),
#         F.col("mc_lvl2_code"), F.col("mc_lvl2_desc"),
#         F.col("mc_lvl3_code"), F.col("mc_lvl3_desc"),
#         F.col("valid_from"),
#         F.col("valid_to"),
#         F.col("version"),
#     )
# )

# print(f"dim_products rows: {dim_products.count():,}")
# dim_products.display()

# COMMAND ----------

# dim_products.groupBy("item_code").count().orderBy("count", ascending=False).display(10)

# COMMAND ----------

# dim_products.filter(F.col("item_code") == "A012D02590004").display()

# COMMAND ----------

# (
#     dim_products.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(TARGET_DIM_PRODUCTS)
# )

# print(f"✓ Written to {TARGET_DIM_PRODUCTS}")