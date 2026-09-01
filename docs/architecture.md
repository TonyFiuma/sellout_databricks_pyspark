# Architecture notes

This full portfolio edition deliberately keeps the original engineering layers and source
heterogeneity. Source-specific parsing/import logic feeds Bronze landing tables. Silver then
centralizes reference-data preparation, FX handling, harmonization and valuation logic.
Gold is an added business-serving layer that resolves resubmissions and produces curated
Sell-Out reporting outputs.

Gold is deliberately denormalized because the rich Silver output already carries the required
business context. A star schema could be introduced for a specific BI requirement, but it is
not treated as synonymous with the Medallion Gold layer.
