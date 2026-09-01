# Databricks notebook source
# Public portfolio defaults only. Override at deployment time when needed.
CATALOG = "sellout_portfolio"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
GOVERNANCE_SCHEMA = "governance"

HARMONIZED_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.harmonized"
GOLD_SELLOUT_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.sellout_curated"
GOLD_COMPANY_KPI_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.sellout_company_kpi"
GOLD_PRODUCT_KPI_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.sellout_product_kpi"
GOLD_MARKET_KPI_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.sellout_market_kpi"
