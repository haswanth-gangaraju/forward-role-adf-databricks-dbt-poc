"""
Tests for Databricks Bronze ingestion and Silver transformation modules.
"""
import pytest
from datetime import datetime
from databricks_notebooks.bronze_ingest import (
    ColumnSpec,
    TableSchema,
    BRONZE_METADATA_COLUMNS,
    enforce_schema,
    compute_record_hash,
    add_bronze_metadata,
    ingest_batch,
    get_ingestion_stats,
    build_delta_write_config,
    generate_bronze_ddl,
)
from databricks_notebooks.silver_transform import (
    DQRule,
    DQResult,
    not_null_rule,
    positive_numeric_rule,
    max_length_rule,
    value_in_set_rule,
    date_not_future_rule,
    SilverTransformProcessor,
    deduplicate_records,
    detect_changed_records,
    ReconciliationResult,
    reconcile_row_counts,
    reconcile_column_checksums,
    get_silver_transform_stats,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def customer_schema():
    return TableSchema(
        table_name="customers",
        source_schema="dbo",
        columns=[
            ColumnSpec("customer_id", "int", nullable=False),
            ColumnSpec("customer_name", "string", nullable=False),
            ColumnSpec("customer_email", "string", nullable=True),
            ColumnSpec("customer_status", "string", nullable=False),
        ],
    )


@pytest.fixture
def sample_customer_record():
    return {
        "customer_id": 1,
        "customer_name": "Acme Corp",
        "customer_email": "acme@test.com",
        "customer_status": "ACTIVE",
    }


@pytest.fixture
def sample_customers():
    return [
        {"customer_id": 1, "customer_name": "Acme Corp", "customer_email": "acme@test.com", "customer_status": "ACTIVE"},
        {"customer_id": 2, "customer_name": "Barton Ltd", "customer_email": None, "customer_status": "INACTIVE"},
        {"customer_id": 3, "customer_name": "Clarke Mfg", "customer_email": "clarke@test.com", "customer_status": "ACTIVE"},
    ]


@pytest.fixture
def sample_orders():
    return [
        {"order_id": 1, "customer_id": 1, "total_amount": 500.0, "order_status": "COMPLETED"},
        {"order_id": 2, "customer_id": 2, "total_amount": 1200.0, "order_status": "PENDING"},
        {"order_id": 3, "customer_id": 1, "total_amount": -50.0, "order_status": "CANCELLED"},
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: ColumnSpec
# ─────────────────────────────────────────────────────────────────────────────

class TestColumnSpec:
    def test_column_spec_creation(self):
        col = ColumnSpec("id", "int", nullable=False)
        assert col.name == "id"
        assert col.dtype == "int"
        assert col.nullable is False

    def test_column_spec_defaults(self):
        col = ColumnSpec("name", "string")
        assert col.nullable is True
        assert col.default is None

    def test_column_spec_with_default(self):
        col = ColumnSpec("status", "string", nullable=False, default="ACTIVE")
        assert col.default == "ACTIVE"


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: TableSchema
# ─────────────────────────────────────────────────────────────────────────────

class TestTableSchema:
    def test_schema_column_names(self, customer_schema):
        names = customer_schema.column_names()  # method call
        assert "customer_id" in names
        assert "customer_name" in names

    def test_schema_required_columns(self, customer_schema):
        required = customer_schema.required_columns()  # method call
        assert "customer_id" in required
        assert "customer_name" in required
        assert "customer_email" not in required  # nullable

    def test_schema_table_name(self, customer_schema):
        assert customer_schema.table_name == "customers"

    def test_schema_source_schema(self, customer_schema):
        assert customer_schema.source_schema == "dbo"


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: BRONZE_METADATA_COLUMNS
# ─────────────────────────────────────────────────────────────────────────────

class TestBronzeMetadataColumns:
    def test_metadata_columns_exist(self):
        assert isinstance(BRONZE_METADATA_COLUMNS, list)
        assert len(BRONZE_METADATA_COLUMNS) >= 3

    def test_metadata_has_ingestion_timestamp(self):
        names = [c.name for c in BRONZE_METADATA_COLUMNS]
        assert "_ingestion_timestamp" in names

    def test_metadata_has_source_table(self):
        names = [c.name for c in BRONZE_METADATA_COLUMNS]
        assert "_source_table" in names

    def test_metadata_has_record_hash(self):
        names = [c.name for c in BRONZE_METADATA_COLUMNS]
        assert "_record_hash" in names

    def test_metadata_has_is_deleted(self):
        names = [c.name for c in BRONZE_METADATA_COLUMNS]
        assert "_is_deleted" in names


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: enforce_schema
# ─────────────────────────────────────────────────────────────────────────────

class TestEnforceSchema:
    def test_enforce_schema_valid_record(self, sample_customer_record, customer_schema):
        result = enforce_schema(sample_customer_record, customer_schema)
        assert result["customer_id"] == 1
        assert result["customer_name"] == "Acme Corp"

    def test_enforce_schema_raises_for_null_required(self, customer_schema):
        bad_record = {"customer_id": None, "customer_name": "Test", "customer_status": "ACTIVE"}
        with pytest.raises(ValueError):
            enforce_schema(bad_record, customer_schema)

    def test_enforce_schema_coerces_int(self, customer_schema):
        record = {"customer_id": "10", "customer_name": "Test Corp", "customer_status": "ACTIVE"}
        result = enforce_schema(record, customer_schema)
        assert result["customer_id"] == 10

    def test_enforce_schema_nullable_column_can_be_none(self, customer_schema):
        record = {"customer_id": 1, "customer_name": "Test", "customer_email": None, "customer_status": "ACTIVE"}
        result = enforce_schema(record, customer_schema)
        assert result["customer_email"] is None

    def test_enforce_schema_drops_undeclared_columns(self, customer_schema):
        record = {
            "customer_id": 1,
            "customer_name": "Test",
            "customer_status": "ACTIVE",
            "extra_column": "should be dropped",
        }
        result = enforce_schema(record, customer_schema)
        assert "extra_column" not in result


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: compute_record_hash
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeRecordHash:
    def test_hash_is_string(self, sample_customer_record):
        h = compute_record_hash(sample_customer_record)
        assert isinstance(h, str)

    def test_hash_consistent(self, sample_customer_record):
        h1 = compute_record_hash(sample_customer_record)
        h2 = compute_record_hash(sample_customer_record)
        assert h1 == h2

    def test_different_records_different_hash(self, sample_customers):
        h1 = compute_record_hash(sample_customers[0])
        h2 = compute_record_hash(sample_customers[1])
        assert h1 != h2

    def test_hash_excludes_metadata_columns(self):
        r1 = {"customer_id": 1, "_ingestion_timestamp": "2024-01-01"}
        r2 = {"customer_id": 1, "_ingestion_timestamp": "2024-06-01"}
        assert compute_record_hash(r1) == compute_record_hash(r2)

    def test_hash_is_md5_length(self, sample_customer_record):
        h = compute_record_hash(sample_customer_record)
        assert len(h) == 32  # MD5 hex digest


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: add_bronze_metadata
# ─────────────────────────────────────────────────────────────────────────────

class TestAddBronzeMetadata:
    def test_metadata_added(self, sample_customer_record):
        enriched = add_bronze_metadata(sample_customer_record, source_table="customers")
        assert "_ingestion_timestamp" in enriched
        assert "_source_table" in enriched
        assert "_record_hash" in enriched
        assert "_is_deleted" in enriched

    def test_source_table_set(self, sample_customer_record):
        enriched = add_bronze_metadata(sample_customer_record, source_table="customers")
        assert enriched["_source_table"] == "customers"

    def test_is_deleted_defaults_false(self, sample_customer_record):
        enriched = add_bronze_metadata(sample_customer_record, source_table="customers")
        assert enriched["_is_deleted"] is False

    def test_original_fields_preserved(self, sample_customer_record):
        enriched = add_bronze_metadata(sample_customer_record, source_table="customers")
        assert enriched["customer_id"] == 1
        assert enriched["customer_name"] == "Acme Corp"

    def test_source_file_optional(self, sample_customer_record):
        enriched = add_bronze_metadata(
            sample_customer_record, source_table="customers", source_file="raw/customers.parquet"
        )
        assert enriched["_source_file"] == "raw/customers.parquet"


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: ingest_batch
# ─────────────────────────────────────────────────────────────────────────────

class TestIngestBatch:
    def test_ingest_batch_returns_list(self, sample_customers, customer_schema):
        result = ingest_batch(sample_customers, schema=customer_schema)
        assert isinstance(result, list)

    def test_ingest_batch_all_valid(self, sample_customers, customer_schema):
        result = ingest_batch(sample_customers, schema=customer_schema)
        assert len(result) == 3

    def test_ingest_batch_adds_metadata(self, sample_customers, customer_schema):
        result = ingest_batch(sample_customers, schema=customer_schema)
        for record in result:
            assert "_ingestion_timestamp" in record
            assert "_record_hash" in record

    def test_ingest_batch_skips_invalid(self, customer_schema):
        records = [
            {"customer_id": None, "customer_name": "Bad", "customer_status": "ACTIVE"},  # required null → error
            {"customer_id": 2, "customer_name": "Good", "customer_status": "ACTIVE"},
        ]
        result = ingest_batch(records, schema=customer_schema)
        assert len(result) == 1
        assert result[0]["customer_id"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: get_ingestion_stats
# ─────────────────────────────────────────────────────────────────────────────

class TestGetIngestionStats:
    def test_perfect_run(self):
        stats = get_ingestion_stats(raw_count=10, bronze_count=10, error_count=0)
        assert stats["success_rate_pct"] == 100.0
        assert stats["passed"] is True

    def test_partial_failure(self):
        stats = get_ingestion_stats(raw_count=10, bronze_count=8, error_count=2)
        assert stats["success_rate_pct"] == 80.0
        assert stats["passed"] is False

    def test_zero_raw_count(self):
        stats = get_ingestion_stats(raw_count=0, bronze_count=0, error_count=0)
        assert stats["success_rate_pct"] == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Bronze: build_delta_write_config & generate_bronze_ddl
# ─────────────────────────────────────────────────────────────────────────────

class TestDeltaAndDDL:
    def test_build_delta_write_config(self):
        cfg = build_delta_write_config(
            table_name="bronze_customers",
            delta_path="/mnt/bronze/customers",
        )
        assert cfg.get("format") == "delta"

    def test_build_delta_write_config_default_mode(self):
        cfg = build_delta_write_config(table_name="t", delta_path="/p")
        assert cfg.get("mode") == "append"

    def test_build_delta_write_config_invalid_mode(self):
        with pytest.raises(ValueError):
            build_delta_write_config(table_name="t", delta_path="/p", write_mode="upsert")

    def test_build_delta_write_config_overwrite(self):
        cfg = build_delta_write_config(table_name="t", delta_path="/p", write_mode="overwrite")
        assert cfg["mode"] == "overwrite"

    def test_build_delta_write_config_partition_by(self):
        cfg = build_delta_write_config(
            table_name="t", delta_path="/p", partition_by="ingestion_date"
        )
        assert "partitionBy" in cfg
        assert "ingestion_date" in cfg["partitionBy"]

    def test_generate_bronze_ddl(self, customer_schema):
        ddl = generate_bronze_ddl(customer_schema, delta_path="/mnt/bronze/customers")
        assert isinstance(ddl, str)
        assert "customer_id" in ddl
        assert "USING DELTA" in ddl

    def test_generate_bronze_ddl_has_metadata_cols(self, customer_schema):
        ddl = generate_bronze_ddl(customer_schema, delta_path="/mnt/bronze/customers")
        assert "_ingestion_timestamp" in ddl
        assert "_record_hash" in ddl


# ─────────────────────────────────────────────────────────────────────────────
# Silver: DQRule
# ─────────────────────────────────────────────────────────────────────────────

class TestDQRule:
    def test_rule_name_and_column(self):
        rule = not_null_rule("customer_name")
        assert rule.name == "not_null_customer_name"
        assert rule.column == "customer_name"

    def test_rule_check_passes(self):
        rule = not_null_rule("customer_name")
        assert rule.check("Acme Corp") is True

    def test_rule_check_fails(self):
        rule = not_null_rule("customer_name")
        assert rule.check(None) is False

    def test_rule_fix_applied(self):
        rule = positive_numeric_rule("amount")
        fixed = rule.fix(-100.0)
        assert fixed == 100.0


# ─────────────────────────────────────────────────────────────────────────────
# Silver: Standard DQ Rule Library
# ─────────────────────────────────────────────────────────────────────────────

class TestDQRules:
    def test_not_null_passes_value(self):
        assert not_null_rule("col").check("value") is True

    def test_not_null_fails_none(self):
        assert not_null_rule("col").check(None) is False

    def test_not_null_fails_empty_string(self):
        assert not_null_rule("col").check("") is False

    def test_positive_numeric_passes(self):
        assert positive_numeric_rule("amount").check(100.0) is True

    def test_positive_numeric_fails_negative(self):
        assert positive_numeric_rule("amount").check(-50.0) is False

    def test_positive_numeric_fails_zero(self):
        assert positive_numeric_rule("amount").check(0) is False

    def test_max_length_passes(self):
        assert max_length_rule("name", 200).check("Short Name") is True

    def test_max_length_fails(self):
        assert max_length_rule("name", 5).check("Very Long Name") is False

    def test_max_length_passes_none(self):
        # None is allowed by max_length (check: v is None or ...)
        assert max_length_rule("name", 5).check(None) is True

    def test_value_in_set_passes(self):
        rule = value_in_set_rule("status", {"ACTIVE", "INACTIVE"})
        assert rule.check("ACTIVE") is True

    def test_value_in_set_fails(self):
        rule = value_in_set_rule("status", {"ACTIVE", "INACTIVE"})
        assert rule.check("UNKNOWN") is False

    def test_date_not_future_passes_string(self):
        rule = date_not_future_rule("order_date")
        # Pass a past date as ISO string
        assert rule.check("2020-01-01T00:00:00") is True

    def test_date_not_future_fails_future_string(self):
        rule = date_not_future_rule("order_date")
        assert rule.check("2099-12-31T00:00:00") is False

    def test_date_not_future_passes_none(self):
        rule = date_not_future_rule("order_date")
        assert rule.check(None) is True


# ─────────────────────────────────────────────────────────────────────────────
# Silver: SilverTransformProcessor
# ─────────────────────────────────────────────────────────────────────────────

class TestSilverTransformProcessor:
    @pytest.fixture
    def rules(self):
        return [
            not_null_rule("customer_id"),
            not_null_rule("customer_name"),
        ]

    @pytest.fixture
    def order_rules(self):
        return [
            not_null_rule("order_id"),
            positive_numeric_rule("total_amount"),
            value_in_set_rule("order_status", {"COMPLETED", "PENDING", "SHIPPED", "CANCELLED", "RETURNED"}),
        ]

    def test_apply_rules_returns_tuple(self, sample_customer_record, rules):
        processor = SilverTransformProcessor(rules=rules)
        result = processor.apply_rules(sample_customer_record)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_apply_rules_all_pass(self, sample_customer_record, rules):
        processor = SilverTransformProcessor(rules=rules)
        transformed, dq_results = processor.apply_rules(sample_customer_record)
        assert all(r.passed for r in dq_results)

    def test_process_batch_returns_triple(self, sample_orders, order_rules):
        processor = SilverTransformProcessor(rules=order_rules)
        result = processor.process_batch(sample_orders)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_process_batch_filters_failed_error_severity(self, sample_orders, order_rules):
        processor = SilverTransformProcessor(rules=order_rules)
        clean, rejected, error_counts = processor.process_batch(sample_orders)
        # order_id=3 has negative total_amount → positive_numeric_rule severity=warning, NOT error
        # So it should be in CLEAN (warnings don't reject)
        # order_id=1 and order_id=2 should be clean
        all_ids = [r["order_id"] for r in clean + rejected]
        assert 1 in all_ids
        assert 2 in all_ids

    def test_process_batch_rejects_null_required(self, rules):
        records = [
            {"customer_id": None, "customer_name": "Test"},  # null required → error → rejected
            {"customer_id": 2, "customer_name": "Good"},
        ]
        processor = SilverTransformProcessor(rules=rules)
        clean, rejected, _ = processor.process_batch(records)
        assert len(clean) == 1
        assert len(rejected) == 1
        assert rejected[0]["customer_id"] is None

    def test_process_batch_error_counts(self, rules):
        records = [
            {"customer_id": None, "customer_name": None},
            {"customer_id": 2, "customer_name": "Good"},
        ]
        processor = SilverTransformProcessor(rules=rules)
        _, _, error_counts = processor.process_batch(records)
        assert isinstance(error_counts, dict)
        assert sum(error_counts.values()) >= 2


# ─────────────────────────────────────────────────────────────────────────────
# Silver: deduplicate_records
# ─────────────────────────────────────────────────────────────────────────────

class TestDeduplicateRecords:
    def test_dedup_removes_duplicates(self):
        records = [
            {"order_id": 1, "amount": 100, "_ingestion_timestamp": "2024-01-01"},
            {"order_id": 1, "amount": 100, "_ingestion_timestamp": "2024-01-01"},
            {"order_id": 2, "amount": 200, "_ingestion_timestamp": "2024-01-01"},
        ]
        result = deduplicate_records(records, key_columns=["order_id"])
        assert len(result) == 2

    def test_dedup_keeps_latest_by_order_column(self):
        records = [
            {"order_id": 1, "amount": 100, "_ingestion_timestamp": "2024-01-01"},
            {"order_id": 1, "amount": 150, "_ingestion_timestamp": "2024-06-01"},
        ]
        result = deduplicate_records(records, key_columns=["order_id"])
        assert len(result) == 1
        assert result[0]["amount"] == 150

    def test_dedup_composite_key(self):
        records = [
            {"customer_id": 1, "product_id": 10, "qty": 5, "_ingestion_timestamp": "T1"},
            {"customer_id": 1, "product_id": 10, "qty": 8, "_ingestion_timestamp": "T2"},
            {"customer_id": 1, "product_id": 20, "qty": 3, "_ingestion_timestamp": "T1"},
        ]
        result = deduplicate_records(records, key_columns=["customer_id", "product_id"])
        assert len(result) == 2

    def test_dedup_no_duplicates_unchanged(self):
        records = [
            {"id": 1, "_ingestion_timestamp": "2024-01-01"},
            {"id": 2, "_ingestion_timestamp": "2024-01-02"},
            {"id": 3, "_ingestion_timestamp": "2024-01-03"},
        ]
        result = deduplicate_records(records, key_columns=["id"])
        assert len(result) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Silver: detect_changed_records
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectChangedRecords:
    def test_returns_dict_with_all_keys(self):
        result = detect_changed_records(
            current_records=[{"id": 1, "_record_hash": "abc"}],
            previous_records=[{"id": 1, "_record_hash": "abc"}],
            key_column="id",
        )
        assert "new" in result
        assert "updated" in result
        assert "deleted" in result
        assert "unchanged" in result

    def test_detects_new_records(self):
        existing = [{"id": 1, "_record_hash": "abc"}]
        incoming = [
            {"id": 1, "_record_hash": "abc"},
            {"id": 2, "_record_hash": "def"},
        ]
        result = detect_changed_records(
            current_records=incoming, previous_records=existing, key_column="id"
        )
        assert len(result["new"]) == 1
        assert result["new"][0]["id"] == 2

    def test_detects_updated_records(self):
        existing = [{"id": 1, "_record_hash": "abc"}]
        incoming = [{"id": 1, "_record_hash": "xyz"}]  # hash changed
        result = detect_changed_records(
            current_records=incoming, previous_records=existing, key_column="id"
        )
        assert len(result["updated"]) == 1

    def test_detects_deleted_records(self):
        existing = [
            {"id": 1, "_record_hash": "abc"},
            {"id": 2, "_record_hash": "def"},
        ]
        incoming = [{"id": 1, "_record_hash": "abc"}]  # id=2 gone
        result = detect_changed_records(
            current_records=incoming, previous_records=existing, key_column="id"
        )
        assert len(result["deleted"]) == 1

    def test_no_changes_returns_unchanged(self):
        records = [{"id": 1, "_record_hash": "abc"}, {"id": 2, "_record_hash": "def"}]
        result = detect_changed_records(
            current_records=records, previous_records=records, key_column="id"
        )
        assert len(result["unchanged"]) == 2
        assert len(result["new"]) == 0
        assert len(result["updated"]) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Silver: Reconciliation
# ─────────────────────────────────────────────────────────────────────────────

class TestReconcileRowCounts:
    def test_exact_match_passes(self):
        result = reconcile_row_counts(100, 100)
        assert result.passed is True

    def test_mismatch_fails(self):
        result = reconcile_row_counts(100, 90)
        assert result.passed is False

    def test_within_tolerance_passes(self):
        result = reconcile_row_counts(100, 98, tolerance_pct=5.0)
        assert result.passed is True

    def test_beyond_tolerance_fails(self):
        result = reconcile_row_counts(100, 80, tolerance_pct=5.0)
        assert result.passed is False

    def test_result_has_counts(self):
        result = reconcile_row_counts(100, 95)
        assert result.source_count == 100
        assert result.target_count == 95
        assert result.discrepancy == 5


class TestReconcileColumnChecksums:
    def test_matching_sums_pass(self):
        source = [{"amount": 100.0}, {"amount": 200.0}]
        target = [{"amount": 100.0}, {"amount": 200.0}]
        result = reconcile_column_checksums(source, target, numeric_columns=["amount"])
        assert result["amount"]["passed"] is True

    def test_mismatched_sums_fail(self):
        source = [{"amount": 100.0}]
        target = [{"amount": 999.0}]
        result = reconcile_column_checksums(source, target, numeric_columns=["amount"])
        assert result["amount"]["passed"] is False

    def test_result_has_sums(self):
        source = [{"amount": 500.0}]
        target = [{"amount": 500.0}]
        result = reconcile_column_checksums(source, target, numeric_columns=["amount"])
        assert result["amount"]["source_sum"] == 500.0
        assert result["amount"]["target_sum"] == 500.0

    def test_none_values_excluded(self):
        source = [{"amount": 100.0}, {"amount": None}]
        target = [{"amount": 100.0}, {"amount": None}]
        result = reconcile_column_checksums(source, target, numeric_columns=["amount"])
        assert result["amount"]["passed"] is True

    def test_multiple_columns(self):
        source = [{"a": 100.0, "b": 50.0}]
        target = [{"a": 100.0, "b": 50.0}]
        result = reconcile_column_checksums(source, target, numeric_columns=["a", "b"])
        assert "a" in result
        assert "b" in result


class TestGetSilverTransformStats:
    def test_perfect_run(self):
        stats = get_silver_transform_stats(clean_count=10, rejected_count=0, dedup_removed=2)
        assert stats["clean_records"] == 10
        assert stats["rejected_records"] == 0
        assert stats["passed"] is True

    def test_partial_failures(self):
        stats = get_silver_transform_stats(clean_count=8, rejected_count=2, dedup_removed=0)
        assert stats["passed"] is False
        assert stats["rejection_rate_pct"] == 20.0

    def test_input_records_is_sum(self):
        stats = get_silver_transform_stats(clean_count=7, rejected_count=3, dedup_removed=1)
        assert stats["input_records"] == 10

    def test_dedup_removed_tracked(self):
        stats = get_silver_transform_stats(clean_count=10, rejected_count=0, dedup_removed=5)
        assert stats["deduplication_removed"] == 5
