# Governance and privacy

Publication-time sanitization and runtime governance are separate controls.

**Publication-time:** original Git history is removed; company/source identities are replaced
with deterministic aliases; production catalog/path names are replaced; long source sample
identifiers are pseudonymized; the original field-mapping workbook is replaced by a sanitized
fixture containing only the mapping metadata needed by the importers.

**Runtime:** Unity Catalog provides the catalog/schema permission boundary. Production grants
should follow least privilege and be assigned to groups/service principals. The Gold example
includes customer masking and Delta constraints. Tag-driven ABAC is suitable when the same
policy must be centrally reused across many governed objects.
