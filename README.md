# 📊 Sell-Out Data Pipeline — Databricks & PySpark

## Overview

The **Sell-Out Data Pipeline** is an end-to-end data engineering project built on **Azure Databricks** using the **Medallion Architecture (Bronze, Silver, Gold)**.

The pipeline integrates multiple heterogeneous and legacy data sources, applies source-specific parsing and business transformations, standardizes reference data and exchange rates, performs data-quality controls, and publishes business-ready Sell-Out datasets for analytics and reporting.

This repository is a **sanitized portfolio version of a real-world project**. Production data, customer identities, environment details, original Git history, proprietary samples, and sensitive source information have been removed or pseudonymized.

> **Note:** the Gold layer is a portfolio extension designed to demonstrate a realistic business-serving layer. It does not claim to reproduce the original production Gold implementation.

---

## 🛠 Technologies Used

- **Azure Databricks** — data processing and orchestration
- **PySpark** — distributed ETL and transformation logic
- **Delta Lake** — reliable Lakehouse tables
- **Databricks SQL** — Gold aggregations and analytical serving
- **Unity Catalog** — governance, access control and masking examples
- **Python** — parsing, utility functions and data-quality logic
- **Auto Loader** — incremental file-ingestion pattern
- **Databricks Asset Bundles** — job/resource definition
- **Medallion Architecture** — Bronze, Silver and Gold separation

---

## 🏗 Architecture

```text
Raw files / Historical datasets
             |
             v
          BRONZE
   Raw ingestion + landing
             |
     +-------+--------+
     |                |
     v                v
Source parsers   Legacy importers
     |                |
     +-------+--------+
             |
             v
          SILVER
   Reference normalization
   Customer/channel mapping
   Country/currency mapping
   Product/company enrichment
   Exchange-rate normalization
   Harmonization
   Sell-In valorization
   Sell-Out valorization
   Data Quality
             |
             v
           GOLD
      sellout_curated
       /      |       \
      v       v        v
 Company   Product   Market/Channel
   KPI       KPI          KPI
             |
             v
      SQL / BI / Reporting
```

---

## 🥉 Bronze Layer

The Bronze layer preserves source-aligned data and ingestion metadata with minimal transformation.

### Main responsibilities

- Raw file ingestion
- Landing-table creation
- Ingestion metadata tracking
- Batch traceability
- Support for heterogeneous historical formats

The project contains a large set of anonymized legacy importers and multiple source-specific parsers, reflecting the complexity of integrating data delivered by different partners and formats.

---

## 🥈 Silver Layer

The Silver layer converts heterogeneous source data into a reusable and consistent analytical dataset.

### Main transformations

- Country and currency normalization
- Customer and sales-channel mapping
- Product and company reference enrichment
- Exchange-rate preparation and FX normalization
- Schema harmonization across heterogeneous sources
- Sell-In valorization
- Sell-Out valorization
- Mapping-status and validation controls
- Data-quality checks

The central output is the harmonized dataset used by downstream business logic.

---

## 🥇 Gold Layer

The Gold layer is implemented as a **business-oriented serving layer** rather than an artificial star schema.

Silver already provides a rich harmonized dataset, so Gold focuses on reporting rules and analytical outputs.

### Latest-submission rule

A partner can resend data for a previously submitted reporting period. Before publishing Gold, the pipeline keeps only the latest submission for each:

```text
company + reporting_month
```

using the maximum sortable `submission_id`.

This avoids double counting when a previous submission is replaced by a newer one.

### Gold datasets

| Dataset | Purpose |
|---|---|
| `gold.sellout_curated` | Detailed business-ready Sell-Out dataset |
| `gold.sellout_company_kpi` | Monthly company performance and month-over-month trend |
| `gold.sellout_product_kpi` | Product performance and market reach |
| `gold.sellout_market_kpi` | Country and sales-channel performance |

### Example KPIs

- Total Sales
- Total Quantity
- Average Sales per Unit
- Active Products
- Active Markets
- Active Customers
- Month-over-Month Sales Growth

---

## ✅ Data Quality

Data quality is applied across the pipeline rather than only at the end.

Examples include:

- Mandatory-field validation
- Mapping completeness
- Negative quantity/sales detection
- Submission consistency
- Duplicate prevention
- Gold output validation
- Referential and schema controls

The project also contains automated repository tests to prevent accidental publication of obvious environment identifiers or sensitive references.

---

## 🔐 Governance

The project demonstrates governance patterns based on **Unity Catalog**.

Key concepts include:

- Catalog and schema separation
- Least-privilege access
- Group-oriented permissions
- Managed Delta tables
- Column masking example for customer identifiers
- Delta `NOT NULL` and `CHECK` constraints
- Optional ABAC / governed-tag strategy
- Separation between repository sanitization and runtime data masking

No production secrets or workspace/storage credentials are stored in source control.

---

## 📁 Repository Structure

```text
src/
├── ingestion/
│   ├── 00_ingestion_raw_files.py
│   ├── legacy_import/
│   └── parsing_files/
│
├── silver/
│   ├── 02_ref_country_currency.py
│   ├── 02_ref_customers_channels.py
│   ├── 02_ref_dimensions.py
│   ├── 02_ref_exchange_rates.py
│   ├── 02_ref_fx_normalized.py
│   ├── 03_harmonize.py
│   ├── 04_valorize_sellin.py
│   └── 05_valorize_sellout.py
│
├── gold/
│   ├── 01_build_sellout_curated.py
│   ├── 02_build_business_kpis.py
│   └── 03_quality_checks.py
│
├── governance/
└── common/

resources/
docs/
tests/
sample_data/
```

---

## 🔄 Pipeline Workflow

```text
Raw ingestion
     ↓
Source parsing / legacy import
     ↓
Reference preparation
     ↓
FX normalization
     ↓
Harmonization
     ↓
Sell-In / Sell-Out valorization
     ↓
Silver Data Quality
     ↓
Latest-submission selection
     ↓
Gold curated dataset
     ↓
Business KPIs
     ↓
Gold Data Quality
```

---

## 🚀 How to Use

### 1. Configure Databricks

Use a Unity Catalog-enabled Databricks workspace and review the portfolio-safe configuration under `src/common/`.

### 2. Review governance setup

Execute or adapt the SQL scripts under:

```text
src/governance/
```

with an appropriately privileged deployment identity.

### 3. Run the pipeline

The repository includes a Databricks Asset Bundle definition and job resources. The notebooks can also be executed manually in dependency order for demonstration purposes.

### 4. Validate Gold

Run the Gold quality checks after generating the curated and KPI tables.

---

## 🔒 Portfolio & Data Privacy Notice

This repository preserves the engineering complexity of the source project while protecting confidential information.

The following have been removed or replaced:

- Production company and customer names
- Production datasets
- Original Git history
- Workspace/catalog/storage identifiers
- Proprietary source samples
- Sensitive mapping values
- Hard-coded execution identifiers

Source organizations are represented using deterministic aliases such as `company_001`, `company_002`, etc., allowing the technical relationships to remain understandable without exposing real identities.

For additional details, see `SANITIZATION_REPORT.md`.

---

## 🎯 Project Goal

The goal of this repository is to demonstrate practical experience in building a production-style Databricks data pipeline involving:

- heterogeneous source integration;
- distributed transformations with PySpark;
- reference-data management;
- data quality;
- financial/FX normalization;
- Medallion architecture;
- business-oriented Gold modeling;
- governance with Unity Catalog.

