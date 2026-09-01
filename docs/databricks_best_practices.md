# Databricks design notes

The portfolio implementation follows these design choices:

- **Bronze** remains source-aligned and traceable rather than applying business cleanup early.
- **Silver** performs validation, reference enrichment, normalization, FX handling and reusable harmonization.
- **Gold** publishes semantically meaningful business datasets and aggregates for reporting.
- Unity Catalog is used as the governance boundary for catalogs, schemas, tables, masks and grants.
- The public repository contains no production secrets or production datasets; runtime masking is defense in depth, not a replacement for publication-time sanitization.
- Gold managed tables use `CLUSTER BY AUTO` where supported instead of introducing manual partitions without workload evidence.
- Table-specific masking is shown for clarity; centrally managed ABAC policies are preferable when the same policy must scale across many tagged objects.

The Gold layer is intentionally a denormalized serving model because the project Silver layer already contains rich reusable business context. A dimensional model would be introduced only when justified by a specific BI/semantic requirement.
