"""
Databricks Bronze Layer — Raw Ingestion
Reads Parquet files landed by ADF from SQL Server into ADLS Gen2,
applies schema enforcement, adds ingestion metadata, and writes to Delta.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Schema enforcement
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnSpec:
    """Column specification for schema enforcement."""
    name: str
    dtype: str          # string, int, long, double, decimal, boolean, timestamp, date
    nullable: bool = True
    default: Any = None


@dataclass
class TableSchema:
    """Full schema definition for a source table."""
    table_name: str
    source_schema: str
    columns: List[ColumnSpec] = field(default_factory=list)

    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    def required_columns(self) -> List[str]:
        return [c.name for c in self.columns if not c.nullable]


# Standard metadata columns added to every Bronze table
BRONZE_METADATA_COLUMNS = [
    ColumnSpec("_ingestion_timestamp", "timestamp", nullable=False),
    ColumnSpec("_source_file", "string", nullable=True),
    ColumnSpec("_source_table", "string", nullable=False),
    ColumnSpec("_record_hash", "string", nullable=False),
    ColumnSpec("_is_deleted", "boolean", nullable=False, default=False),
]


# ─────────────────────────────────────────────────────────────────────────────
# Bronze ingestion logic
# ─────────────────────────────────────────────────────────────────────────────

def enforce_schema(
    record: Dict[str, Any],
    schema: TableSchema
) -> Dict[str, Any]:
    """
    Apply schema enforcement to a raw record.
    - Drops undeclared columns
    - Coerces declared columns to their target types
    - Fills nulls for nullable columns with defaults
    - Raises ValueError for missing required columns
    """
    enforced: Dict[str, Any] = {}
    for col in schema.columns:
        value = record.get(col.name)
        if value is None and not col.nullable:
            raise ValueError(f"Required column '{col.name}' is null in record")
        if value is None:
            enforced[col.name] = col.default
        else:
            enforced[col.name] = _coerce_type(value, col.dtype, col.name)
    return enforced


def _coerce_type(value: Any, dtype: str, column_name: str) -> Any:
    """Coerce a value to the target Spark dtype."""
    try:
        if dtype in ("string",):
            return str(value)
        if dtype in ("int",):
            return int(value)
        if dtype in ("long",):
            return int(value)
        if dtype in ("double", "float"):
            return float(value)
        if dtype in ("boolean",):
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes")
        if dtype in ("decimal",):
            return float(value)
        if dtype in ("timestamp", "date"):
            if isinstance(value, (datetime,)):
                return value
            return value  # pass through as string for now
        return value
    except (TypeError, ValueError) as e:
        raise TypeError(f"Cannot coerce column '{column_name}' value '{value}' to {dtype}: {e}") from e


def compute_record_hash(record: Dict[str, Any]) -> str:
    """
    Compute a deterministic MD5 hash of a record's business key fields.
    Used for deduplication and change detection in Silver layer.
    """
    # Exclude metadata columns from hash computation
    business_fields = {
        k: v for k, v in sorted(record.items())
        if not k.startswith("_")
    }
    content = "&".join(f"{k}={v}" for k, v in sorted(business_fields.items()))
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def add_bronze_metadata(
    record: Dict[str, Any],
    source_table: str,
    source_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    Add standard Bronze metadata columns to a record.
    These columns are added to every Bronze table for lineage and deduplication.
    """
    enriched = dict(record)
    enriched["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    enriched["_source_table"] = source_table
    enriched["_source_file"] = source_file or ""
    enriched["_record_hash"] = compute_record_hash(record)
    enriched["_is_deleted"] = False
    return enriched


def ingest_batch(
    records: List[Dict[str, Any]],
    schema: TableSchema,
    source_file: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Process a batch of raw records through Bronze ingestion:
    1. Schema enforcement
    2. Metadata enrichment
    3. Returns list of Bronze-ready records
    """
    bronze_records: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for i, record in enumerate(records):
        try:
            enforced = enforce_schema(record, schema)
            enriched = add_bronze_metadata(enriched := enforced, schema.table_name, source_file)
            bronze_records.append(enriched)
        except (ValueError, TypeError) as e:
            errors.append({"record_index": i, "error": str(e), "raw": record})

    return bronze_records


def get_ingestion_stats(
    raw_count: int,
    bronze_count: int,
    error_count: int
) -> Dict[str, Any]:
    """Generate ingestion statistics for monitoring."""
    success_rate = (bronze_count / raw_count * 100) if raw_count > 0 else 0.0
    return {
        "raw_records": raw_count,
        "bronze_records": bronze_count,
        "error_records": error_count,
        "success_rate_pct": round(success_rate, 2),
        "passed": error_count == 0
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bronze Delta write simulation
# ─────────────────────────────────────────────────────────────────────────────

def build_delta_write_config(
    table_name: str,
    delta_path: str,
    write_mode: str = "append",
    partition_by: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build a Delta table write configuration.
    In production, passed to spark.write.format('delta').
    """
    if write_mode not in ("append", "overwrite", "merge"):
        raise ValueError(f"Invalid write_mode '{write_mode}'. Must be append, overwrite, or merge.")

    config: Dict[str, Any] = {
        "format": "delta",
        "mode": write_mode,
        "path": delta_path,
        "table_name": table_name,
        "options": {
            "mergeSchema": "true",
            "overwriteSchema": "false"
        }
    }
    if partition_by:
        config["partitionBy"] = [partition_by]
    return config


def generate_bronze_ddl(schema: TableSchema, delta_path: str) -> str:
    """
    Generate a CREATE TABLE DDL statement for a Bronze Delta table.
    Includes metadata columns.
    """
    all_columns = list(schema.columns) + BRONZE_METADATA_COLUMNS
    col_defs = []
    for col in all_columns:
        null_str = "" if col.nullable else " NOT NULL"
        col_defs.append(f"  {col.name} {col.dtype.upper()}{null_str}")
    cols_str = ",\n".join(col_defs)
    return (
        f"CREATE TABLE IF NOT EXISTS bronze.{schema.table_name} (\n"
        f"{cols_str}\n"
        f") USING DELTA\n"
        f"LOCATION '{delta_path}'\n"
        f"TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');"
    )
