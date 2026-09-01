-- Enforced Delta constraints for critical Gold invariants.
ALTER TABLE sellout_portfolio.gold.sellout_curated ALTER COLUMN date SET NOT NULL;
ALTER TABLE sellout_portfolio.gold.sellout_curated ALTER COLUMN company SET NOT NULL;
ALTER TABLE sellout_portfolio.gold.sellout_curated ALTER COLUMN submission_id SET NOT NULL;
ALTER TABLE sellout_portfolio.gold.sellout_curated ADD CONSTRAINT valid_quantity CHECK (quantity >= 0);
