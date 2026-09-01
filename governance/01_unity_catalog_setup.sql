-- Databricks SQL / Unity Catalog
CREATE CATALOG IF NOT EXISTS sellout_portfolio
COMMENT 'Public portfolio catalog with synthetic/pseudonymized data only';

CREATE SCHEMA IF NOT EXISTS sellout_portfolio.bronze COMMENT 'Raw/source-aligned layer';
CREATE SCHEMA IF NOT EXISTS sellout_portfolio.silver COMMENT 'Validated/harmonized layer';
CREATE SCHEMA IF NOT EXISTS sellout_portfolio.gold COMMENT 'Business serving layer';
CREATE SCHEMA IF NOT EXISTS sellout_portfolio.governance COMMENT 'Governance functions/policies';

-- Grant privileges to account groups/service principals in a real deployment.
-- Avoid grants to individual users unless there is a specific operational reason.
