"""
Databricks Silver Layer — Cleansing, Deduplication & Reconciliation
Reads Bronze Delta tables, applies data quality rules, deduplicates records,
and produces clean Silver tables ready for dbt transformation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# Data quality rules
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DQRule:
    """A single data quality rule applied to a column or record."""
    name: str
    column: str
    check_fn: Callable[[Any], bool]
    fix_fn: Optional[Callable[[Any], Any]] = None
    severity: str = "error"   # error | warning

    def check(self, value: Any) -> bool:
        try:
            return self.check_fn(value)
        except Exception:
            return False

    def fix(self, value: Any) -> Any:
        if self.fix_fn:
            try:
                return self.fix_fn(value)
            except Exception:
                return value
        return value


@dataclass
class DQResult:
    """Result of a data quality check on a single record."""
    rule_name: str
    column: str
    passed: bool
    original_value: Any
    fixed_value: Any = None
    severity: str = "error"


# ─────────────────────────────────────────────────────────────────────────────
# Standard DQ rule library
# ─────────────────────────────────────────────────────────────────────────────

def not_null_rule(column: str) -> DQRule:
    return DQRule(
        name=f"not_null_{column}",
        column=column,
        check_fn=lambda v: v is not None and str(v).strip() != "",
        severity="error"
    )


def positive_numeric_rule(column: str) -> DQRule:
    return DQRule(
        name=f"positive_{column}",
        column=column,
        check_fn=lambda v: v is not None and float(v) > 0,
        fix_fn=lambda v: abs(float(v)) if v is not None else 0.0,
        severity="warning"
    )


def max_length_rule(column: str, max_len: int) -> DQRule:
    return DQRule(
        name=f"max_length_{column}",
        column=column,
        check_fn=lambda v: v is None or len(str(v)) <= max_len,
        fix_fn=lambda v: str(v)[:max_len] if v is not None else v,
        severity="warning"
    )


def value_in_set_rule(column: str, allowed: set) -> DQRule:
    return DQRule(
        name=f"allowed_values_{column}",
        column=column,
        check_fn=lambda v: v in allowed,
        severity="error"
    )


def date_not_future_rule(column: str) -> DQRule:
    from datetime import datetime
    return DQRule(
        name=f"not_future_{column}",
        column=column,
        check_fn=lambda v: v is None or (isinstance(v, str) and v <= datetime.now().isoformat()),
        severity="warning"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Silver transformation processor
# ─────────────────────────────────────────────────────────────────────────────

class SilverTransformProcessor:
    """
    Applies DQ rules and transformations to Bronze records to produce Silver output.
    """

    def __init__(self, rules: List[DQRule]):
        self.rules = rules

    def apply_rules(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[DQResult]]:
        """
        Apply all DQ rules to a record.
        Returns (transformed_record, list_of_dq_results).
        """
        transformed = dict(record)
        results: List[DQResult] = []

        for rule in self.rules:
            value = transformed.get(rule.column)
            passed = rule.check(value)

            if not passed and rule.fix_fn is not None:
                fixed = rule.fix(value)
                transformed[rule.column] = fixed
            else:
                fixed = value

            results.append(DQResult(
                rule_name=rule.name,
                column=rule.column,
                passed=passed,
                original_value=value,
                fixed_value=fixed,
                severity=rule.severity
            ))

        return transformed, results

    def process_batch(
        self,
        records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
        """
        Process a batch of Bronze records through Silver transformation.
        Returns: (clean_records, rejected_records, stats)
        """
        clean: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        error_counts: Dict[str, int] = {}

        for record in records:
            transformed, dq_results = self.apply_rules(record)
            has_errors = any(r.severity == "error" and not r.passed for r in dq_results)

            for r in dq_results:
                if not r.passed:
                    error_counts[r.rule_name] = error_counts.get(r.rule_name, 0) + 1

            if has_errors:
                record_copy = dict(transformed)
                record_copy["_dq_errors"] = [
                    {"rule": r.rule_name, "column": r.column, "value": str(r.original_value)}
                    for r in dq_results if not r.passed and r.severity == "error"
                ]
                rejected.append(record_copy)
            else:
                clean.append(transformed)

        return clean, rejected, error_counts


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate_records(
    records: List[Dict[str, Any]],
    key_columns: List[str],
    order_column: str = "_ingestion_timestamp"
) -> List[Dict[str, Any]]:
    """
    Deduplicate records by business key, keeping the most recent version.
    Simulates a Spark window function RANK() OVER (PARTITION BY key ORDER BY order_column DESC) = 1.
    """
    seen: Dict[tuple, Dict[str, Any]] = {}

    for record in records:
        key = tuple(record.get(col) for col in key_columns)
        existing = seen.get(key)

        if existing is None:
            seen[key] = record
        else:
            # Keep the record with the latest order_column value
            current_order = str(record.get(order_column, ""))
            existing_order = str(existing.get(order_column, ""))
            if current_order > existing_order:
                seen[key] = record

    return list(seen.values())


def detect_changed_records(
    current_records: List[Dict[str, Any]],
    previous_records: List[Dict[str, Any]],
    key_column: str,
    hash_column: str = "_record_hash"
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Detect new, updated, and deleted records between two snapshots.
    Returns dict with keys: "new", "updated", "deleted", "unchanged"
    """
    current_by_key = {r[key_column]: r for r in current_records}
    previous_by_key = {r[key_column]: r for r in previous_records}

    new_records: List[Dict[str, Any]] = []
    updated_records: List[Dict[str, Any]] = []
    deleted_records: List[Dict[str, Any]] = []
    unchanged_records: List[Dict[str, Any]] = []

    for key, record in current_by_key.items():
        if key not in previous_by_key:
            new_records.append(record)
        elif record.get(hash_column) != previous_by_key[key].get(hash_column):
            updated_records.append(record)
        else:
            unchanged_records.append(record)

    for key, record in previous_by_key.items():
        if key not in current_by_key:
            deleted_copy = dict(record)
            deleted_copy["_is_deleted"] = True
            deleted_records.append(deleted_copy)

    return {
        "new": new_records,
        "updated": updated_records,
        "deleted": deleted_records,
        "unchanged": unchanged_records
    }


# ─────────────────────────────────────────────────────────────────────────────
# Reconciliation
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReconciliationResult:
    """Result of a source-to-target reconciliation check."""
    source_count: int
    target_count: int
    count_match: bool
    discrepancy: int
    discrepancy_pct: float
    passed: bool
    message: str


def reconcile_row_counts(
    source_count: int,
    target_count: int,
    tolerance_pct: float = 0.0
) -> ReconciliationResult:
    """
    Compare source and target row counts with optional tolerance.
    """
    discrepancy = abs(source_count - target_count)
    discrepancy_pct = (discrepancy / source_count * 100) if source_count > 0 else 100.0
    count_match = discrepancy_pct <= tolerance_pct
    passed = count_match

    if count_match:
        msg = f"PASS: row counts match within {tolerance_pct}% tolerance (source={source_count}, target={target_count})"
    else:
        msg = f"FAIL: row count mismatch — source={source_count}, target={target_count}, discrepancy={discrepancy} ({discrepancy_pct:.1f}%)"

    return ReconciliationResult(
        source_count=source_count,
        target_count=target_count,
        count_match=count_match,
        discrepancy=discrepancy,
        discrepancy_pct=round(discrepancy_pct, 2),
        passed=passed,
        message=msg
    )


def reconcile_column_checksums(
    source_records: List[Dict[str, Any]],
    target_records: List[Dict[str, Any]],
    numeric_columns: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Compare aggregate checksums (SUM) for numeric columns between source and target.
    """
    results: Dict[str, Dict[str, Any]] = {}

    for col in numeric_columns:
        source_sum = sum(
            float(r.get(col, 0) or 0)
            for r in source_records
            if r.get(col) is not None
        )
        target_sum = sum(
            float(r.get(col, 0) or 0)
            for r in target_records
            if r.get(col) is not None
        )
        diff = abs(source_sum - target_sum)
        passed = diff < 0.01  # floating point tolerance

        results[col] = {
            "source_sum": round(source_sum, 4),
            "target_sum": round(target_sum, 4),
            "difference": round(diff, 4),
            "passed": passed,
            "message": "PASS" if passed else f"FAIL: sum mismatch ({diff:.4f} difference)"
        }

    return results


def get_silver_transform_stats(
    clean_count: int,
    rejected_count: int,
    dedup_removed: int
) -> Dict[str, Any]:
    """Generate Silver transformation statistics."""
    total = clean_count + rejected_count
    return {
        "input_records": total,
        "clean_records": clean_count,
        "rejected_records": rejected_count,
        "deduplication_removed": dedup_removed,
        "rejection_rate_pct": round(rejected_count / total * 100, 2) if total > 0 else 0.0,
        "passed": rejected_count == 0
    }
