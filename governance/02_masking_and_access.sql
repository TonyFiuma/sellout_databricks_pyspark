-- Runtime masking example; repository data itself is already pseudonymized.
CREATE OR REPLACE FUNCTION sellout_portfolio.governance.mask_customer(value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('portfolio_privileged') THEN value
  WHEN value IS NULL THEN NULL
  ELSE concat('CUSTOMER_', right(sha2(value, 256), 10))
END;

ALTER TABLE sellout_portfolio.gold.sellout_curated
  ALTER COLUMN customer
  SET MASK sellout_portfolio.governance.mask_customer;

-- For larger estates, consider centrally managed Unity Catalog ABAC policies
-- driven by governed tags rather than maintaining many table-specific policies.
