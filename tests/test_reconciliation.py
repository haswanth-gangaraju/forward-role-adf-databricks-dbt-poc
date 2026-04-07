"""
Tests for the Source-to-Target Reconciliation Framework (validation/reconciliation.py).
"""
import pytest
from validation.reconciliation import (
    ReconciliationStatus,
    ReconciliationCheck,
    ReconciliationReport,
    check_row_count_reconciliation,
    check_column_sum_reconciliation,
    check_null_rate_reconciliation,
    check_distinct_values_reconciliation,
    check_primary_key_completeness,
    run_full_reconciliation,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def source_records():
    return [
        {"order_id": 1, "customer_id": 10, "total_amount": 500.0,  "region": "NORTH"},
        {"order_id": 2, "customer_id": 20, "total_amount": 1200.0, "region": "SOUTH"},
        {"order_id": 3, "customer_id": 10, "total_amount": 800.0,  "region": "EAST"},
        {"order_id": 4, "customer_id": 30, "total_amount": None,   "region": None},
    ]


@pytest.fixture
def target_records(source_records):
    # Identical copy for "happy path" tests
    return list(source_records)


# ─────────────────────────────────────────────────────────────────────────────
# ReconciliationStatus enum
# ─────────────────────────────────────────────────────────────────────────────

class TestReconciliationStatus:
    def test_pass_value(self):
        assert ReconciliationStatus.PASS.value == "PASS"

    def test_warn_value(self):
        assert ReconciliationStatus.WARN.value == "WARN"

    def test_fail_value(self):
        assert ReconciliationStatus.FAIL.value == "FAIL"

    def test_skip_value(self):
        assert ReconciliationStatus.SKIP.value == "SKIP"


# ─────────────────────────────────────────────────────────────────────────────
# ReconciliationCheck dataclass
# ─────────────────────────────────────────────────────────────────────────────

class TestReconciliationCheck:
    def test_create_check(self):
        check = ReconciliationCheck(
            check_name="test_check",
            status=ReconciliationStatus.PASS,
            source_value=100,
            target_value=100,
            discrepancy=0,
            message="All good",
        )
        assert check.check_name == "test_check"
        assert check.status == ReconciliationStatus.PASS
        assert check.message == "All good"

    def test_default_layer_is_silver(self):
        check = ReconciliationCheck(
            check_name="test",
            status=ReconciliationStatus.PASS,
            source_value=1,
            target_value=1,
            discrepancy=0,
            message="ok",
        )
        assert check.layer == "silver"

    def test_custom_layer(self):
        check = ReconciliationCheck(
            check_name="test",
            status=ReconciliationStatus.PASS,
            source_value=1,
            target_value=1,
            discrepancy=0,
            message="ok",
            layer="gold",
        )
        assert check.layer == "gold"


# ─────────────────────────────────────────────────────────────────────────────
# ReconciliationReport
# ─────────────────────────────────────────────────────────────────────────────

class TestReconciliationReport:
    def test_empty_report_all_zero(self):
        report = ReconciliationReport(table_name="orders")
        assert report.passed == 0
        assert report.failed == 0
        assert report.warned == 0

    def test_passed_count(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.PASS, 10, 10, 0, "ok"))
        report.checks.append(ReconciliationCheck("c2", ReconciliationStatus.PASS, 5, 5, 0, "ok"))
        assert report.passed == 2

    def test_failed_count(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.FAIL, 10, 8, 2, "fail"))
        assert report.failed == 1

    def test_warned_count(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.WARN, 10, 9, 1, "warn"))
        assert report.warned == 1

    def test_overall_status_pass_all_pass(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.PASS, 10, 10, 0, "ok"))
        assert report.overall_status == ReconciliationStatus.PASS

    def test_overall_status_fail_with_any_fail(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.PASS, 10, 10, 0, "ok"))
        report.checks.append(ReconciliationCheck("c2", ReconciliationStatus.FAIL, 10, 8, 2, "fail"))
        assert report.overall_status == ReconciliationStatus.FAIL

    def test_overall_status_warn_no_fail(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.PASS, 10, 10, 0, "ok"))
        report.checks.append(ReconciliationCheck("c2", ReconciliationStatus.WARN, 10, 9, 1, "warn"))
        assert report.overall_status == ReconciliationStatus.WARN

    def test_summary_contains_table_name(self):
        report = ReconciliationReport(table_name="orders")
        assert "orders" in report.summary()

    def test_summary_contains_overall_status(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.PASS, 1, 1, 0, "ok"))
        summary = report.summary()
        assert "PASS" in summary

    def test_summary_contains_counts(self):
        report = ReconciliationReport(table_name="orders")
        report.checks.append(ReconciliationCheck("c1", ReconciliationStatus.PASS, 1, 1, 0, "ok"))
        report.checks.append(ReconciliationCheck("c2", ReconciliationStatus.FAIL, 1, 0, 1, "fail"))
        summary = report.summary()
        assert "1" in summary  # at least one pass and one fail


# ─────────────────────────────────────────────────────────────────────────────
# check_row_count_reconciliation
# ─────────────────────────────────────────────────────────────────────────────

class TestRowCountReconciliation:
    def test_exact_match_passes(self):
        check = check_row_count_reconciliation(100, 100, "orders")
        assert check.status == ReconciliationStatus.PASS

    def test_mismatch_fails(self):
        check = check_row_count_reconciliation(100, 90, "orders")
        assert check.status == ReconciliationStatus.FAIL

    def test_within_tolerance_warns(self):
        check = check_row_count_reconciliation(100, 98, "orders", tolerance_pct=5.0)
        assert check.status == ReconciliationStatus.WARN

    def test_beyond_tolerance_fails(self):
        check = check_row_count_reconciliation(100, 80, "orders", tolerance_pct=5.0)
        assert check.status == ReconciliationStatus.FAIL

    def test_both_zero_passes(self):
        check = check_row_count_reconciliation(0, 0, "orders")
        assert check.status == ReconciliationStatus.PASS

    def test_zero_source_nonzero_target_fails(self):
        check = check_row_count_reconciliation(0, 10, "orders")
        assert check.status == ReconciliationStatus.FAIL

    def test_check_name_includes_table(self):
        check = check_row_count_reconciliation(10, 10, "my_table")
        assert "my_table" in check.check_name

    def test_layer_parameter(self):
        check = check_row_count_reconciliation(10, 10, "orders", layer="gold")
        assert check.layer == "gold"

    def test_source_value_stored(self):
        check = check_row_count_reconciliation(100, 95, "orders")
        assert check.source_value == 100

    def test_target_value_stored(self):
        check = check_row_count_reconciliation(100, 95, "orders")
        assert check.target_value == 95

    def test_discrepancy_stored(self):
        check = check_row_count_reconciliation(100, 90, "orders")
        assert check.discrepancy == 10


# ─────────────────────────────────────────────────────────────────────────────
# check_column_sum_reconciliation
# ─────────────────────────────────────────────────────────────────────────────

class TestColumnSumReconciliation:
    def test_exact_match_passes(self, source_records, target_records):
        check = check_column_sum_reconciliation(
            source_records, target_records, "total_amount", "orders"
        )
        assert check.status == ReconciliationStatus.PASS

    def test_mismatch_fails(self, source_records):
        modified = [dict(r, total_amount=(r.get("total_amount") or 0) + 1000)
                    for r in source_records]
        check = check_column_sum_reconciliation(
            source_records, modified, "total_amount", "orders"
        )
        assert check.status == ReconciliationStatus.FAIL

    def test_none_values_treated_as_zero(self, source_records, target_records):
        # Both have total_amount=None for order_id=4 — should still pass
        check = check_column_sum_reconciliation(
            source_records, target_records, "total_amount", "orders"
        )
        assert check.status == ReconciliationStatus.PASS

    def test_check_name_includes_column_and_table(self):
        s = [{"amount": 100}]
        t = [{"amount": 100}]
        check = check_column_sum_reconciliation(s, t, "amount", "orders")
        assert "amount" in check.check_name
        assert "orders" in check.check_name

    def test_source_and_target_sums_stored(self, source_records, target_records):
        check = check_column_sum_reconciliation(
            source_records, target_records, "total_amount", "orders"
        )
        # Sum of 500 + 1200 + 800 + 0 (None treated as 0) = 2500
        assert check.source_value == 2500.0
        assert check.target_value == 2500.0

    def test_discrepancy_is_zero_on_match(self, source_records, target_records):
        check = check_column_sum_reconciliation(
            source_records, target_records, "total_amount", "orders"
        )
        assert check.discrepancy == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# check_null_rate_reconciliation
# ─────────────────────────────────────────────────────────────────────────────

class TestNullRateReconciliation:
    def test_same_null_rate_passes(self, source_records, target_records):
        check = check_null_rate_reconciliation(
            source_records, target_records, "region", "orders"
        )
        assert check.status == ReconciliationStatus.PASS

    def test_target_more_nulls_within_tolerance_warns(self, source_records):
        # source: 1/4 nulls (25%). target: 3/4 nulls (75%). diff = 50% > 5% tolerance
        target = [
            {"order_id": 1, "region": None},
            {"order_id": 2, "region": None},
            {"order_id": 3, "region": "EAST"},
            {"order_id": 4, "region": None},
        ]
        check = check_null_rate_reconciliation(
            source_records, target, "region", "orders", tolerance_pct=5.0
        )
        assert check.status == ReconciliationStatus.FAIL

    def test_target_fewer_nulls_passes(self, source_records):
        # All non-null in target → diff is negative (fewer nulls) → PASS
        target = [
            {"order_id": 1, "region": "NORTH"},
            {"order_id": 2, "region": "SOUTH"},
            {"order_id": 3, "region": "EAST"},
            {"order_id": 4, "region": "WEST"},
        ]
        check = check_null_rate_reconciliation(
            source_records, target, "region", "orders"
        )
        assert check.status == ReconciliationStatus.PASS

    def test_check_name_includes_column(self, source_records, target_records):
        check = check_null_rate_reconciliation(
            source_records, target_records, "region", "orders"
        )
        assert "region" in check.check_name

    def test_source_and_target_rates_stored(self, source_records, target_records):
        check = check_null_rate_reconciliation(
            source_records, target_records, "region", "orders"
        )
        # source has 1 null out of 4 → 25.0%
        assert check.source_value == 25.0
        assert check.target_value == 25.0


# ─────────────────────────────────────────────────────────────────────────────
# check_distinct_values_reconciliation
# ─────────────────────────────────────────────────────────────────────────────

class TestDistinctValuesReconciliation:
    def test_exact_match_passes(self, source_records, target_records):
        check = check_distinct_values_reconciliation(
            source_records, target_records, "region", "orders"
        )
        assert check.status == ReconciliationStatus.PASS

    def test_lost_values_fails(self, source_records):
        # Drop EAST from target
        target = [r for r in source_records if r["region"] != "EAST"]
        check = check_distinct_values_reconciliation(
            source_records, target, "region", "orders"
        )
        assert check.status == ReconciliationStatus.FAIL

    def test_new_values_in_target_warns(self, source_records):
        target = list(source_records) + [{"order_id": 5, "region": "WALES"}]
        check = check_distinct_values_reconciliation(
            source_records, target, "region", "orders"
        )
        assert check.status == ReconciliationStatus.WARN

    def test_discrepancy_has_lost_and_new(self, source_records):
        target = [{"region": "NORTH"}, {"region": "SOUTH"}]
        check = check_distinct_values_reconciliation(
            source_records, target, "region", "orders"
        )
        assert isinstance(check.discrepancy, dict)
        assert "lost" in check.discrepancy
        assert "new" in check.discrepancy
        assert check.discrepancy["lost"] >= 1

    def test_none_excluded_from_distinct(self):
        source = [{"col": "A"}, {"col": None}, {"col": "B"}]
        target = [{"col": "A"}, {"col": "B"}]
        check = check_distinct_values_reconciliation(source, target, "col", "test_table")
        assert check.status == ReconciliationStatus.PASS


# ─────────────────────────────────────────────────────────────────────────────
# check_primary_key_completeness
# ─────────────────────────────────────────────────────────────────────────────

class TestPrimaryKeyCompleteness:
    def test_all_pks_present_passes(self, source_records, target_records):
        check = check_primary_key_completeness(
            source_records, target_records, "order_id", "orders"
        )
        assert check.status == ReconciliationStatus.PASS

    def test_missing_pk_fails(self, source_records):
        target = [r for r in source_records if r["order_id"] != 3]
        check = check_primary_key_completeness(
            source_records, target, "order_id", "orders"
        )
        assert check.status == ReconciliationStatus.FAIL

    def test_extra_pks_in_target_still_passes(self, source_records):
        target = list(source_records) + [{"order_id": 99, "region": "MIDLANDS"}]
        check = check_primary_key_completeness(
            source_records, target, "order_id", "orders"
        )
        assert check.status == ReconciliationStatus.PASS

    def test_discrepancy_has_missing_and_extra(self, source_records):
        target = [r for r in source_records if r["order_id"] != 3]
        check = check_primary_key_completeness(
            source_records, target, "order_id", "orders"
        )
        assert isinstance(check.discrepancy, dict)
        assert check.discrepancy["missing"] == 1

    def test_check_name_includes_pk_column(self, source_records, target_records):
        check = check_primary_key_completeness(
            source_records, target_records, "order_id", "orders"
        )
        assert "order_id" in check.check_name

    def test_none_pks_excluded(self):
        source = [{"id": 1}, {"id": 2}, {"id": None}]
        target = [{"id": 1}, {"id": 2}]
        check = check_primary_key_completeness(source, target, "id", "test")
        # None is excluded from source keys, so 2 source keys, 2 target keys → PASS
        assert check.status == ReconciliationStatus.PASS


# ─────────────────────────────────────────────────────────────────────────────
# run_full_reconciliation
# ─────────────────────────────────────────────────────────────────────────────

class TestRunFullReconciliation:
    def test_returns_report_instance(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
        )
        assert isinstance(report, ReconciliationReport)

    def test_table_name_set(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
        )
        assert report.table_name == "orders"

    def test_row_count_check_present(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
        )
        names = [c.check_name for c in report.checks]
        assert any("row_count" in n for n in names)

    def test_pk_completeness_check_present(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
        )
        names = [c.check_name for c in report.checks]
        assert any("pk_completeness" in n for n in names)

    def test_numeric_column_sum_check_added(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
            numeric_columns=["total_amount"],
        )
        names = [c.check_name for c in report.checks]
        assert any("col_sum" in n and "total_amount" in n for n in names)

    def test_null_rate_check_added(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
            nullable_columns=["region"],
        )
        names = [c.check_name for c in report.checks]
        assert any("null_rate" in n and "region" in n for n in names)

    def test_distinct_check_added(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
            distinct_columns=["region"],
        )
        names = [c.check_name for c in report.checks]
        assert any("distinct_values" in n and "region" in n for n in names)

    def test_all_pass_on_identical_data(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
            numeric_columns=["total_amount"],
            nullable_columns=["region"],
            distinct_columns=["region"],
        )
        assert report.failed == 0
        assert report.overall_status in (ReconciliationStatus.PASS, ReconciliationStatus.WARN)

    def test_fails_on_row_count_mismatch(self, source_records):
        target = source_records[:2]
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target,
            table_name="orders",
            pk_column="order_id",
        )
        assert report.overall_status == ReconciliationStatus.FAIL

    def test_layer_applied_to_all_checks(self, source_records, target_records):
        report = run_full_reconciliation(
            source_records=source_records,
            target_records=target_records,
            table_name="orders",
            pk_column="order_id",
            layer="gold",
        )
        for check in report.checks:
            assert check.layer == "gold"

    def test_multiple_numeric_columns(self, source_records, target_records):
        source = [dict(r, customer_id=float(r["customer_id"])) for r in source_records]
        target = [dict(r, customer_id=float(r["customer_id"])) for r in target_records]
        report = run_full_reconciliation(
            source_records=source,
            target_records=target,
            table_name="orders",
            pk_column="order_id",
            numeric_columns=["total_amount", "customer_id"],
        )
        names = [c.check_name for c in report.checks]
        assert any("customer_id" in n for n in names)
        assert any("total_amount" in n for n in names)
