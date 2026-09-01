# Databricks notebook source
# Mocked portfolio Gold: business-facing aggregates built from the curated Sell-Out dataset.

# COMMAND ----------
# MAGIC %run ../common/portfolio_config

# COMMAND ----------
source = spark.table(GOLD_SELLOUT_TABLE)
source.createOrReplaceTempView("_portfolio_gold_source")

# KPI inputs use columns already produced by the real Silver flow.
required = {"reporting_month", "year", "semester", "company", "quantity"}
missing = required.difference(source.columns)
if missing:
    raise ValueError(f"Gold KPI input is missing required columns: {sorted(missing)}")

sales_col = "sales_local_currency" if "sales_local_currency" in source.columns else "sales"
country_col = "destination_country" if "destination_country" in source.columns else None
product_col = "item_code" if "item_code" in source.columns else None
customer_col = "customer" if "customer" in source.columns else None
channel_col = "channel" if "channel" in source.columns else None
currency_col = "local_currency" if "local_currency" in source.columns else (
    "billing_currency" if "billing_currency" in source.columns else None
)

if sales_col not in source.columns:
    raise ValueError("Gold KPI input requires sales_local_currency or sales")

currency_select = currency_col if currency_col else "'N/A'"
country_select = country_col if country_col else "'N/A'"
channel_select = channel_col if channel_col else "'N/A'"

active_products = f"COUNT(DISTINCT {product_col})" if product_col else "0"
active_markets = f"COUNT(DISTINCT {country_col})" if country_col else "0"
active_customers = f"COUNT(DISTINCT {customer_col})" if customer_col else "0"

# COMMAND ----------
company_sql = f"""
CREATE OR REPLACE TABLE {GOLD_COMPANY_KPI_TABLE}
CLUSTER BY AUTO
COMMENT 'Monthly company Sell-Out KPIs for the public portfolio'
AS
WITH monthly AS (
    SELECT
        reporting_month,
        year,
        semester,
        company,
        {currency_select} AS currency,
        SUM(quantity) AS total_quantity,
        SUM({sales_col}) AS total_sales,
        {active_products} AS active_products,
        {active_markets} AS active_markets,
        {active_customers} AS active_customers
    FROM _portfolio_gold_source
    GROUP BY reporting_month, year, semester, company, {currency_select}
), trended AS (
    SELECT
        *,
        LAG(total_sales) OVER (
            PARTITION BY company, currency ORDER BY reporting_month
        ) AS previous_month_sales
    FROM monthly
)
SELECT
    *,
    total_sales - previous_month_sales AS sales_delta_vs_previous_month,
    CASE
      WHEN previous_month_sales IS NULL OR previous_month_sales = 0 THEN NULL
      ELSE ROUND((total_sales / previous_month_sales - 1) * 100, 2)
    END AS sales_growth_pct
FROM trended
"""
spark.sql(company_sql)

# COMMAND ----------
if product_col:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_PRODUCT_KPI_TABLE}
    CLUSTER BY AUTO
    COMMENT 'Semester product performance for the public portfolio'
    AS
    SELECT
        year,
        semester,
        {product_col} AS item_code,
        {currency_select} AS currency,
        SUM(quantity) AS total_quantity,
        SUM({sales_col}) AS total_sales,
        ROUND(SUM({sales_col}) / NULLIF(SUM(quantity), 0), 2) AS avg_sales_per_unit,
        {active_markets} AS markets_reached,
        {active_customers} AS active_customers
    FROM _portfolio_gold_source
    GROUP BY year, semester, {product_col}, {currency_select}
    """)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_MARKET_KPI_TABLE}
CLUSTER BY AUTO
COMMENT 'Monthly market/channel Sell-Out performance for the public portfolio'
AS
SELECT
    reporting_month,
    year,
    semester,
    {country_select} AS destination_country,
    {channel_select} AS channel,
    {currency_select} AS currency,
    SUM(quantity) AS total_quantity,
    SUM({sales_col}) AS total_sales,
    COUNT(DISTINCT company) AS active_companies,
    {active_products} AS active_products,
    {active_customers} AS active_customers
FROM _portfolio_gold_source
GROUP BY reporting_month, year, semester, {country_select}, {channel_select}, {currency_select}
""")
