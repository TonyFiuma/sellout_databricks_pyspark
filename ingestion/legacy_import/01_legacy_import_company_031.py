# Databricks notebook source
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile
import re

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

if "CATALOG" not in globals():
    CATALOG = "sellout_portfolio"

if "BRONZE_SCHEMA" not in globals():
    BRONZE_SCHEMA = "bronze"

# COMMAND ----------

# =========================================
# CONFIGURATION
# =========================================

COMPANY_NAME = "company_031"
COMPANY_FILE_NAME = "company_031"

SOURCE_PATH = (
    f"/Volumes/sellout_portfolio/landing/historical_data/"
    f"tblImportUnivociVal_{COMPANY_FILE_NAME}.parquet"
)
MAPPING_FILE_NAME = "tblImportUnivociVal_parquet_fields.xlsx"
MAPPING_SHEET = "Field Mapping"

TARGET_INGESTION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.landing_company_031"

# Use overwrite for repeatable tests/backfills. Change to "append" to match
# the existing parsing notebooks' final write mode.
WRITE_MODE = "overwrite"
DRY_RUN = False

print(
    f"""
=========================================

{COMPANY_NAME} LEGACY IMPORT
Source Path            : {SOURCE_PATH}
Target Ingestion Table : {TARGET_INGESTION_TABLE}
Write Mode             : {WRITE_MODE}
Dry Run                : {DRY_RUN}

=========================================
"""
)

# COMMAND ----------

# =========================================
# FIELD MAPPING WORKBOOK READER
# =========================================

EXCEL_MAIN_NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
EXCEL_REL_NS = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}
OFFICE_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

SPARK_TYPE_BY_MAPPING_TYPE = {
    "VARCHAR": "string",
    "STRING": "string",
    "TEXT": "string",
    "INTEGER": "int",
    "INT": "int",
    "BIGINT": "long",
    "DOUBLE": "double",
    "FLOAT": "double",
    "DECIMAL": "double",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "BOOLEAN": "boolean",
}

PERIOD_TARGET_COLUMNS = {"date", "year", "month", "semester", "quarter"}


def normalize_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def excel_column_index(cell_reference: str) -> int:
    letters = "".join(ch for ch in cell_reference if ch.isalpha())
    index = 0
    for letter in letters:
        index = index * 26 + ord(letter.upper()) - ord("A") + 1
    return index - 1


def resolve_xl_target(target: str) -> str:
    target = target.lstrip("/")
    return target if target.startswith("xl/") else f"xl/{target}"


def read_shared_strings(archive: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []

    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    return [
        "".join(text.text or "" for text in item.findall(".//main:t", EXCEL_MAIN_NS))
        for item in root.findall("main:si", EXCEL_MAIN_NS)
    ]


def get_worksheet_path(archive: ZipFile, sheet_name: str) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    relationship_by_id = {
        relationship.attrib["Id"]: relationship.attrib["Target"]
        for relationship in relationships.findall("rel:Relationship", EXCEL_REL_NS)
    }

    for sheet in workbook.findall("main:sheets/main:sheet", EXCEL_MAIN_NS):
        if sheet.attrib["name"] == sheet_name:
            relationship_id = sheet.attrib[f"{{{OFFICE_REL_NS}}}id"]
            return resolve_xl_target(relationship_by_id[relationship_id])

    raise ValueError(f"Sheet not found in mapping workbook: {sheet_name}")


def read_cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")

    if cell_type == "inlineStr":
        return "".join(text.text or "" for text in cell.findall(".//main:t", EXCEL_MAIN_NS))

    value_node = cell.find("main:v", EXCEL_MAIN_NS)
    if value_node is None:
        return ""

    value = value_node.text or ""
    if cell_type == "s" and value:
        return shared_strings[int(value)]
    return value


def read_xlsx_sheet(path: Path, sheet_name: str) -> list[dict[str, str]]:
    with ZipFile(path) as archive:
        shared_strings = read_shared_strings(archive)
        worksheet_path = get_worksheet_path(archive, sheet_name)
        worksheet = ET.fromstring(archive.read(worksheet_path))

        rows = []
        for row in worksheet.findall("main:sheetData/main:row", EXCEL_MAIN_NS):
            values = []
            for cell in row.findall("main:c", EXCEL_MAIN_NS):
                column_index = excel_column_index(cell.attrib.get("r", "A1"))
                while len(values) <= column_index:
                    values.append("")
                values[column_index] = read_cell_value(cell, shared_strings)
            rows.append(values)

    if not rows:
        return []

    max_columns = max(len(row) for row in rows)
    rows = [row + [""] * (max_columns - len(row)) for row in rows]
    header = [value.strip() for value in rows[0]]

    return [
        {column: value.strip() for column, value in zip(header, row)}
        for row in rows[1:]
        if any(value.strip() for value in row)
    ]


def read_field_mapping(path: Path, sheet_name: str) -> dict[str, dict[str, str]]:
    required_columns = {"original_field", "proposed_field", "data_types_found"}
    rows = read_xlsx_sheet(path, sheet_name)

    if not rows:
        raise ValueError(f"No rows found in mapping sheet: {sheet_name}")

    missing_columns = required_columns.difference(rows[0])
    if missing_columns:
        raise ValueError(
            f"Mapping sheet {sheet_name} is missing columns: {sorted(missing_columns)}"
        )

    mapping = {}
    conflicts = []

    for row in rows:
        original_field = row.get("original_field", "").strip()
        proposed_field = row.get("proposed_field", "").strip()
        data_type = row.get("data_types_found", "").strip().upper()

        if not original_field or not proposed_field or not data_type:
            continue

        key = normalize_field_name(original_field)
        candidate = {
            "original_field": original_field,
            "proposed_field": proposed_field,
            "data_types_found": data_type,
        }

        existing = mapping.get(key)
        if existing is None:
            mapping[key] = candidate
            continue

        same_target = (
            existing["proposed_field"] == proposed_field
            and existing["data_types_found"] == data_type
        )
        if not same_target:
            conflicts.append(
                f"{original_field}: {existing['proposed_field']}/"
                f"{existing['data_types_found']} vs {proposed_field}/{data_type}"
            )

    if conflicts:
        raise ValueError("Conflicting field mapping rows found: " + "; ".join(conflicts))

    return mapping


def spark_type_for(mapping_type: str) -> str:
    spark_type = SPARK_TYPE_BY_MAPPING_TYPE.get(mapping_type.upper())
    if spark_type is None:
        raise ValueError(f"Unsupported data_types_found value: {mapping_type}")
    return spark_type


def quote_column(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def source_column_for_target(
    source_columns: list[str],
    field_mapping: dict[str, dict[str, str]],
    target_column: str,
) -> str | None:
    for source_column in source_columns:
        mapping_row = field_mapping.get(normalize_field_name(source_column))
        if mapping_row and mapping_row["proposed_field"] == target_column:
            return source_column
    return None


def regexp_extract_int(source_column: str, pattern: str):
    extracted = F.regexp_extract(F.col(quote_column(source_column)).cast("string"), pattern, 1)
    return F.when(extracted != "", extracted.cast("int"))


def month_from_quarter(source_column: str | None):
    if source_column is None:
        return F.lit(None).cast("int")

    quarter_number = regexp_extract_int(source_column, r"(?i)q([1-4])")
    return (
        F.when(quarter_number == 1, F.lit(1))
        .when(quarter_number == 2, F.lit(4))
        .when(quarter_number == 3, F.lit(7))
        .when(quarter_number == 4, F.lit(10))
        .cast("int")
    )


def month_from_semester(source_column: str | None):
    if source_column is None:
        return F.lit(None).cast("int")

    semester_number = regexp_extract_int(source_column, r"(?i)h([12])")
    return (
        F.when(semester_number == 1, F.lit(1))
        .when(semester_number == 2, F.lit(7))
        .cast("int")
    )


def month_from_month(source_column: str | None):
    if source_column is None:
        return F.lit(None).cast("int")

    month_number = regexp_extract_int(
        source_column,
        r"(?:^|[^0-9])(0?[1-9]|1[0-2])(?:[^0-9]|$)",
    )
    return F.when(
        month_number.between(1, 12),
        month_number,
    ).cast("int")


def parsed_date_column(source_column: str):
    value = F.trim(F.regexp_replace(F.col(quote_column(source_column)).cast("string"), "T", " "))
    return F.coalesce(
        F.to_date(value, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_date(value, "yyyy-MM-dd HH:mm:ss"),
        F.to_date(value, "yyyy-MM-dd"),
        F.to_date(value, "dd/MM/yyyy"),
        F.to_date(value, "d/M/yyyy"),
        F.to_date(value, "dd-MM-yyyy"),
        F.to_date(value, "d-M-yyyy"),
        F.to_date(value, "MM/dd/yyyy"),
        F.to_date(value, "M/d/yyyy"),
    )


def build_date_column(
    source_columns: list[str],
    field_mapping: dict[str, dict[str, str]],
):
    existing_date_column = source_column_for_target(source_columns, field_mapping, "date")
    year_column = source_column_for_target(source_columns, field_mapping, "year")
    month_column = source_column_for_target(source_columns, field_mapping, "month")
    semester_column = source_column_for_target(source_columns, field_mapping, "semester")
    quarter_column = source_column_for_target(source_columns, field_mapping, "quarter")

    if year_column is None:
        if existing_date_column is not None:
            return parsed_date_column(existing_date_column).alias("date")
        raise ValueError("Cannot build date: no source column mapped to year or date")

    year_number = regexp_extract_int(year_column, r"([0-9]{4})")
    month_number = F.coalesce(
        month_from_month(month_column),
        month_from_quarter(quarter_column),
        month_from_semester(semester_column),
        F.lit(1),
    )

    date_value = F.when(
        year_number.isNotNull() & month_number.isNotNull(),
        F.to_date(
            F.concat_ws(
                "-",
                year_number.cast("string"),
                F.lpad(month_number.cast("string"), 2, "0"),
                F.lit("01"),
            ),
            "yyyy-MM-dd",
        ),
    )

    if existing_date_column is not None:
        return F.coalesce(parsed_date_column(existing_date_column), date_value).alias("date")

    return date_value.alias("date")


def build_time_frames_column(
    source_columns: list[str],
    field_mapping: dict[str, dict[str, str]],
):
    existing_date_column = source_column_for_target(source_columns, field_mapping, "date")
    year_column = source_column_for_target(source_columns, field_mapping, "year")
    month_column = source_column_for_target(source_columns, field_mapping, "month")
    semester_column = source_column_for_target(source_columns, field_mapping, "semester")
    quarter_column = source_column_for_target(source_columns, field_mapping, "quarter")

    time_frame_candidates = []
    if existing_date_column is not None:
        time_frame_candidates.append(
            F.when(parsed_date_column(existing_date_column).isNotNull(), F.lit("month"))
        )
    if month_column is not None:
        time_frame_candidates.append(
            F.when(month_from_month(month_column).isNotNull(), F.lit("month"))
        )
    if quarter_column is not None:
        time_frame_candidates.append(
            F.when(month_from_quarter(quarter_column).isNotNull(), F.lit("quarter"))
        )
    if semester_column is not None:
        time_frame_candidates.append(
            F.when(month_from_semester(semester_column).isNotNull(), F.lit("half-year"))
        )
    if year_column is not None:
        time_frame_candidates.append(
            F.when(
                regexp_extract_int(year_column, r"([0-9]{4})").isNotNull(),
                F.lit("year"),
            )
        )

    if not time_frame_candidates:
        raise ValueError("Cannot determine time_frames: no period column found")

    return F.coalesce(*time_frame_candidates).cast("string").alias("time_frames")


def add_legacy_metadata_columns(spark_df):
    required_columns = {"time_frames", "quantity"}
    missing_columns = required_columns.difference(spark_df.columns)
    if missing_columns:
        raise ValueError(
            "Cannot add legacy metadata; missing output columns: "
            + ", ".join(sorted(missing_columns))
        )

    sales_value_column = next(
        (
            column_name
            for column_name in ("gross_sales", "gross_sales_with_charges")
            if column_name in spark_df.columns
        ),
        None,
    )
    if sales_value_column is None:
        raise ValueError("Cannot add legacy metadata; missing gross sales output column")

    selected_columns = []
    for column_name in spark_df.columns:
        if column_name == "time_frames":
            continue

        if column_name == "quantity":
            selected_columns.extend(
                [
                    F.col(quote_column("time_frames")),
                    F.lit(None).cast("double").alias("volumes"),
                ]
            )

        selected_columns.append(F.col(quote_column(column_name)))

        if column_name == sales_value_column:
            selected_columns.extend(
                [
                    F.lit(None).cast("string").alias("data_matrix"),
                    F.lit("sell-out").cast("string").alias("data_collection"),
                    F.lit("sell-out").cast("string").alias("project"),
                    F.lit(Path(SOURCE_PATH).name).cast("string").alias("file_name"),
                ]
            )

    return spark_df.select(*selected_columns)


def notebook_directory() -> Path:
    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        workspace_path = Path("/Workspace") / notebook_path.strip("/")
        return workspace_path.parent
    except Exception:
        return Path.cwd()


def resolve_mapping_path(file_name: str) -> Path:
    candidates = [
        notebook_directory() / file_name,
        Path.cwd() / file_name,
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Mapping workbook not found. Checked: "
        + ", ".join(str(candidate) for candidate in candidates)
    )

# COMMAND ----------

# =========================================
# READ PARQUET AND FIELD MAPPING
# =========================================

mapping_path = resolve_mapping_path(MAPPING_FILE_NAME)
field_mapping = read_field_mapping(mapping_path, MAPPING_SHEET)

source_df = spark.read.parquet(SOURCE_PATH)

print(f"Mapping file: {mapping_path}")
print(f"Source columns: {len(source_df.columns)}")
display(source_df.limit(10))

# COMMAND ----------

# =========================================
# RENAME AND CAST COLUMNS
# =========================================

selected_columns = [build_date_column(source_df.columns, field_mapping)]
missing_columns = []
target_columns = {}
target_column_order = []

for source_column in source_df.columns:
    mapping_row = field_mapping.get(normalize_field_name(source_column))
    if mapping_row is None:
        missing_columns.append(source_column)
        continue

    target_column = mapping_row["proposed_field"]
    if target_column in PERIOD_TARGET_COLUMNS:
        continue

    spark_type = spark_type_for(mapping_row["data_types_found"])
    if target_column not in target_columns:
        target_column_order.append(target_column)
        target_columns[target_column] = []

    target_columns[target_column].append(
        F.col(quote_column(source_column)).cast(spark_type)
    )

if missing_columns:
    raise ValueError(
        "Missing field mapping for parquet columns: " + ", ".join(missing_columns)
    )

for target_column in target_column_order:
    expressions = target_columns[target_column]
    if len(expressions) == 1:
        selected_columns.append(expressions[0].alias(target_column))
    else:
        print(f"Coalescing {len(expressions)} source columns into {target_column}")
        selected_columns.append(F.coalesce(*expressions).alias(target_column))

spark_df = add_legacy_metadata_columns(
    source_df.select(
        *selected_columns,
        build_time_frames_column(source_df.columns, field_mapping),
    )
)

null_date_count = spark_df.filter(F.col("date").isNull()).count()
if null_date_count:
    raise ValueError(f"Rows with null date after period conversion: {null_date_count}")

spark_df.printSchema()
display(spark_df.limit(10))

# COMMAND ----------

# =========================================
# WRITE TO BRONZE TABLE
# =========================================

if DRY_RUN:
    print(f"Dry run completed. Table not written: {TARGET_INGESTION_TABLE}")
else:
    writer = spark_df.write.format("delta").mode(WRITE_MODE)

    if WRITE_MODE == "overwrite":
        writer = writer.option("overwriteSchema", "true")

    writer.saveAsTable(TARGET_INGESTION_TABLE)

    print(f"✓ Written to {TARGET_INGESTION_TABLE}")
