"""
Source-to-Target Reconciliation Framework.
Validates data fidelity between SQL Server source and Databricks/dbt target layers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class ReconciliationStatus(Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class ReconciliationCheck:
    """Result of a single reconciliation check."""
    check_name: str
    status: ReconciliationStatus
    source_value: Any
    target_value: Any
    discrepancy: Any
    message: str
    layer: str = "silver"       # raw | bronze | silver | gold


@dataclass
class ReconciliationReport:
    """Aggregated reconciliation report across all checks."""
    table_name: str
    checks: List[ReconciliationCheck] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.status == ReconciliationStatus.PASS)

    @property
    def warned(self) -> int:
        return sum(1 for c in self.checks if c.status == ReconciliationStatus.WARN)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c.status == ReconciliationStatus.FAIL)

    @property
    def overall_status(self) -> ReconciliationStatus:
        if any(c.status == ReconciliationStatus.FAIL for c in self.checks):
            return ReconciliationStatus.FAIL
        if any(c.status == ReconciliationStatus.WARN for c in self.checks):
            return ReconciliationStatus.WARN
        return ReconciliationStatus.PASS

    def summary(self) -> str:
        return (
            f"Reconciliation [{self.table_name}]: "
            f"{self.passed} PASS / {self.warned} WARN / {self.failed} FAIL — "
            f"Overall: {self.overall_status.value}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Individual reconciliation checks
# ─────────────────────────────────────────────────────────────────────────────

def check_row_count_reconciliation(
    source_count: int,
    target_count: int,
    table_name: str,
    tolerance_pct: float = 0.0,
    layer: str = "silver"
) -> ReconciliationCheck:
    """Check source and target row counts match within tolerance."""
    discrepancy = abs(source_count - target_count)
    pct_diff = (discrepancy / source_count * 100) if source_count > 0 else 100.0

    if source_count == target_count:
        status = ReconciliationStatus.PASS
        msg = f"Row counts match exactly: {source_count}"
    elif pct_diff <= tolerance_pct:
        status = ReconciliationStatus.WARN
        msg = f"Row count within tolerance: source={source_count}, target={target_count} ({pct_diff:.1f}% diff ≤ {tolerance_pct}% tolerance)"
    else:
        status = ReconciliationStatus.FAIL
        msg = f"Row count mismatch: source={source_count}, target={target_count} ({pct_diff:.1f}% diff > {tolerance_pct}% tolerance)"

    return ReconciliationCheck(
        check_name=f"row_count_{table_name}",
        status=status,
        source_value=source_count,
        target_value=target_count,
        discrepancy=discrepancy,
        message=msg,
        layer=layer
    )


def check_column_sum_reconciliation(
    source_records: List[Dict[str, Any]],
    target_records: List[Dict[str, Any]],
    column: str,
    table_name: str,
    tolerance: float = 0.01,
    layer: str = "silver"
) -> ReconciliationCheck:
    """Check SUM of a numeric column matches between source and target."""
    source_sum = sum(
        float(r.get(column, 0) or 0)
        for r in source_records
    )
    target_sum = sum(
        float(r.get(column, 0) or 0)
        for r in target_records
    )
    diff = abs(source_sum - target_sum)

    if diff <= tolerance:
        status = ReconciliationStatus.PASS
        msg = f"Column SUM matches: {column} = {source_sum:.4f}"
    else:
        status = ReconciliationStatus.FAIL
        msg = f"Column SUM mismatch: {column} — source={source_sum:.4f}, target={target_sum:.4f}, diff={diff:.4f}"

    return ReconciliationCheck(
        check_name=f"col_sum_{column}_{table_name}",
        status=status,
        source_value=round(source_sum, 4),
        target_value=round(target_sum, 4),
        discrepancy=round(diff, 4),
        message=msg,
        layer=layer
    )


def check_null_rate_reconciliation(
    source_records: List[Dict[str, Any]],
    target_records: List[Dict[str, Any]],
    column: str,
    table_name: str,
    tolerance_pct: float = 5.0,
    layer: str = "silver"
) -> ReconciliationCheck:
    """Check that null rates don't significantly increase from source to target."""
    def null_rate(records: List[Dict[str, Any]]) -> float:
        if not records:
            return 0.0
        null_count = sum(1 for r in records if r.get(column) is None)
        return null_count / len(records) * 100

    source_rate = null_rate(source_records)
    target_rate = null_rate(target_records)
    diff = target_rate - source_rate  # positive = more nulls introduced in target

    if diff <= 0:
        status = ReconciliationStatus.PASS
        msg = f"Null rate OK: {column} source={source_rate:.1f}%, target={target_rate:.1f}%"
    elif diff <= tolerance_pct:
        status = ReconciliationStatus.WARN
        msg = f"Null rate slightly higher in target: {column} source={source_rate:.1f}%, target={target_rate:.1f}% (diff={diff:.1f}%)"
    else:
        status = ReconciliationStatus.FAIL
        msg = f"Null rate significantly higher in target: {column} source={source_rate:.1f}%, target={target_rate:.1f}% (diff={diff:.1f}% > {tolerance_pct}% tolerance)"

    return ReconciliationCheck(
        check_name=f"null_rate_{column}_{table_name}",
        status=status,
        source_value=round(source_rate, 2),
        target_value=round(target_rate, 2),
        discrepancy=round(diff, 2),
        message=msg,
        layer=layer
    )


def check_distinct_values_reconciliation(
    source_records: List[Dict[str, Any]],
    target_records: List[Dict[str, Any]],
    column: str,
    table_name: str,
    layer: str = "silver"
) -> ReconciliationCheck:
    """Check that distinct values are preserved from source to target."""
    source_distinct: Set[Any] = {r.get(column) for r in source_records if r.get(column) is not None}
    target_distinct: Set[Any] = {r.get(column) for r in target_records if r.get(column) is not None}

    lost_values = source_distinct - target_distinct
    new_values = target_distinct - source_distinct

    if not lost_values and not new_values:
        status = ReconciliationStatus.PASS
        msg = f"Distinct values match: {column} ({len(source_distinct)} values)"
    elif lost_values:
        status = ReconciliationStatus.FAIL
        msg = f"Lost distinct values in {column}: {list(lost_values)[:5]} (total={len(lost_values)} missing)"
    else:
        status = ReconciliationStatus.WARN
        msg = f"New distinct values in target {column}: {list(new_values)[:5]} (not in source)"

    return ReconciliationCheck(
        check_name=f"distinct_values_{column}_{table_name}",
        status=status,
        source_value=len(source_distinct),
        target_value=len(target_distinct),
        discrepancy={"lost": len(lost_values), "new": len(new_values)},
        message=msg,
        layer=layer
    )


def check_primary_key_completeness(
    source_records: List[Dict[str, Any]],
    target_records: List[Dict[str, Any]],
    pk_column: str,
    table_name: str,
    layer: str = "silver"
) -> ReconciliationCheck:
    """Check that all source primary keys appear in the target."""
    source_keys: Set[Any] = {r[pk_column] for r in source_records if r.get(pk_column) is not None}
    target_keys: Set[Any] = {r[pk_column] for r in target_records if r.get(pk_column) is not None}

    missing_keys = source_keys - target_keys
    extra_keys = target_keys - source_keys

    if not missing_keys:
        status = ReconciliationStatus.PASS
        msg = f"All {len(source_keys)} source PKs present in target"
    else:
        status = ReconciliationStatus.FAIL
        msg = f"Missing PKs in target: {len(missing_keys)} keys absent. Sample: {list(missing_keys)[:3]}"

    return ReconciliationCheck(
        check_name=f"pk_completeness_{pk_column}_{table_name}",
        status=status,
        source_value=len(source_keys),
        target_value=len(target_keys),
        discrepancy={"missing": len(missing_keys), "extra": len(extra_keys)},
        message=msg,
        layer=layer
    )


# ─────────────────────────────────────────────────────────────────────────────
# Full reconciliation suite
# ─────────────────────────────────────────────────────────────────────────────

def run_full_reconciliation(
    source_records: List[Dict[str, Any]],
    target_records: List[Dict[str, Any]],
    table_name: str,
    pk_column: str,
    numeric_columns: Optional[List[str]] = None,
    nullable_columns: Optional[List[str]] = None,
    distinct_columns: Optional[List[str]] = None,
    row_count_tolerance_pct: float = 0.0,
    layer: str = "silver"
) -> ReconciliationReport:
    """
    Run a full reconciliation suite for a source-target table pair.
    """
    report = ReconciliationReport(table_name=table_name)

    # 1. Row count
    report.checks.append(
        check_row_count_reconciliation(
            len(source_records), len(target_records), table_name,
            row_count_tolerance_pct, layer
        )
    )

    # 2. PK completeness
    if pk_column and source_records and target_records:
        if pk_column in source_records[0] and pk_column in target_records[0]:
            report.checks.append(
                check_primary_key_completeness(
                    source_records, target_records, pk_column, table_name, layer
                )
            )

    # 3. Numeric column sums
    for col in (numeric_columns or []):
        report.checks.append(
            check_column_sum_reconciliation(
                source_records, target_records, col, table_name, layer=layer
            )
        )

    # 4. Null rate checks
    for col in (nullable_columns or []):
        report.checks.append(
            check_null_rate_reconciliation(
                source_records, target_records, col, table_name, layer=layer
            )
        )

    # 5. Distinct value checks
    for col in (distinct_columns or []):
        report.checks.append(
            check_distinct_values_reconciliation(
                source_records, target_records, col, table_name, layer=layer
            )
        )

    return report
