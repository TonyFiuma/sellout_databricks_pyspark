# Databricks notebook source
# Catalog
CATALOG = "sellout_portfolio"

# Schemas
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

REFERENCE_CATALOG = "reference_data"
REFERENCE_SCHEMA = "master_data"
UTILS_SCHEMA = "utils"

# Paths
RAW_FILES_PATH = "/Volumes/sellout_portfolio/landing/raw_files"
RAW_INGESTION_CHECKPOINT = (
"/Volumes/sellout_portfolio/landing/checkpoints/raw_files/"
)

RAW_INGESTION_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.raw_ingestion'

# Auto Loader
FILE_FORMAT = "binaryFile"
TRIGGER_ONCE = True
