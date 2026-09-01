# Sanitization report

This repository was generated as a public derivative of a confidential project snapshot.

## Preserved

- all 78 legacy-import notebook implementations;
- all 7 source-specific parsing notebook implementations;
- Bronze raw ingestion logic;
- full Silver reference, FX, harmonization and valorization notebooks;
- shared functions and field mapping configuration;
- notebook-level validation/data-quality logic.

## Removed or replaced

- `.git` history and remote metadata;
- original company/source names in filenames, literals and table names;
- original development catalog and volume paths;
- private reference catalog/schema identifiers;
- long SKU/source sample values where they appeared in code/examples;
- original workbook sample values and file references;
- production datasets (none are shipped here).

## Added

- mocked business-oriented Gold serving layer;
- Unity Catalog governance examples;
- Databricks bundle job skeleton;
- automated public-safety checks;
- portfolio architecture/governance documentation.

Aliases are intentionally one-way in the public repository: the original-to-alias mapping is
not included.
