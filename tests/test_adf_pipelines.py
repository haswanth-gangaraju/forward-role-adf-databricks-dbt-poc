"""
Tests for ADF Pipeline Factory module.
"""
import pytest
import json
from adf_pipelines.pipeline_factory import (
    ADFSourceConfig,
    ADFSinkConfig,
    ADFPipelineMetadata,
    ADFPipelineBuilder,
    ADFOrchestratorPipeline,
    build_standard_ingestion_suite,
    validate_pipeline_structure,
    get_pipeline_metadata,
    count_activities_by_type,
    extract_dependencies,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def source_config():
    return ADFSourceConfig(
        server="sql-server.example.com",
        database="OperationsDB",
        table="Customers",
        schema="dbo",
    )


@pytest.fixture
def incremental_source():
    return ADFSourceConfig(
        server="sql-server.example.com",
        database="OperationsDB",
        table="Orders",
        schema="dbo",
        incremental_column="OrderDate",
        watermark_value="2024-01-01 00:00:00",
    )


@pytest.fixture
def sink_config():
    return ADFSinkConfig(
        storage_account="mystorageacct",
        container="raw",
        folder_path="customers",
        file_format="parquet",
    )


@pytest.fixture
def pipeline_metadata():
    return ADFPipelineMetadata(
        pipeline_name="pl_ingest_customers",
        source_system="sql_server",
        target_zone="raw",
        load_type="full",
    )


@pytest.fixture
def pipeline_builder(pipeline_metadata):
    return ADFPipelineBuilder(metadata=pipeline_metadata)


# ─────────────────────────────────────────────────────────────────────────────
# ADFSourceConfig tests
# ─────────────────────────────────────────────────────────────────────────────

class TestADFSourceConfig:
    def test_basic_creation(self, source_config):
        assert source_config.server == "sql-server.example.com"
        assert source_config.database == "OperationsDB"
        assert source_config.table == "Customers"
        assert source_config.schema == "dbo"

    def test_default_schema_is_dbo(self):
        cfg = ADFSourceConfig(server="s", database="d", table="T")
        assert cfg.schema == "dbo"

    def test_incremental_column_default_none(self, source_config):
        assert source_config.incremental_column is None

    def test_incremental_column_set(self, incremental_source):
        assert incremental_source.incremental_column == "OrderDate"

    def test_watermark_value_set(self, incremental_source):
        assert incremental_source.watermark_value == "2024-01-01 00:00:00"


# ─────────────────────────────────────────────────────────────────────────────
# ADFSinkConfig tests
# ─────────────────────────────────────────────────────────────────────────────

class TestADFSinkConfig:
    def test_default_file_format_is_parquet(self):
        sink = ADFSinkConfig(storage_account="sa", container="c", folder_path="fp")
        assert sink.file_format == "parquet"

    def test_custom_file_format(self):
        sink = ADFSinkConfig(storage_account="sa", container="c", folder_path="fp", file_format="delta")
        assert sink.file_format == "delta"

    def test_partition_by_default_none(self, sink_config):
        assert sink_config.partition_by is None


# ─────────────────────────────────────────────────────────────────────────────
# ADFPipelineMetadata tests
# ─────────────────────────────────────────────────────────────────────────────

class TestADFPipelineMetadata:
    def test_required_fields(self, pipeline_metadata):
        assert pipeline_metadata.pipeline_name == "pl_ingest_customers"
        assert pipeline_metadata.source_system == "sql_server"
        assert pipeline_metadata.target_zone == "raw"
        assert pipeline_metadata.load_type == "full"

    def test_default_owner(self, pipeline_metadata):
        assert pipeline_metadata.owner == "data-engineering"

    def test_tags_default_empty(self, pipeline_metadata):
        assert isinstance(pipeline_metadata.tags, list)


# ─────────────────────────────────────────────────────────────────────────────
# ADFPipelineBuilder — full load
# ─────────────────────────────────────────────────────────────────────────────

class TestADFPipelineBuilderFullLoad:
    def test_build_full_load_returns_dict(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        assert isinstance(result, dict)

    def test_full_load_has_name(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        assert result.get("name") == "pl_ingest_customers"

    def test_full_load_has_activities(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        activities = result.get("properties", {}).get("activities", [])
        assert len(activities) >= 1

    def test_full_load_has_copy_activity(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        activities = result.get("properties", {}).get("activities", [])
        types = [a.get("type") for a in activities]
        assert "Copy" in types

    def test_full_load_json_serialisable(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        json_str = json.dumps(result)
        assert len(json_str) > 100

    def test_to_json_method(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        json_str = pipeline_builder.to_json(result)
        parsed = json.loads(json_str)
        assert "name" in parsed

    def test_full_load_has_parameters(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        params = result.get("properties", {}).get("parameters", {})
        assert "SourceTable" in params

    def test_full_load_adf_type(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        assert result.get("type") == "Microsoft.DataFactory/factories/pipelines"


# ─────────────────────────────────────────────────────────────────────────────
# ADFPipelineBuilder — incremental load
# ─────────────────────────────────────────────────────────────────────────────

class TestADFPipelineBuilderIncremental:
    def test_incremental_pipeline_returns_dict(self, incremental_source, sink_config):
        meta = ADFPipelineMetadata(
            pipeline_name="pl_ingest_orders_incr",
            source_system="sql_server",
            target_zone="raw",
            load_type="incremental",
        )
        builder = ADFPipelineBuilder(metadata=meta)
        result = builder.build_incremental_pipeline(incremental_source, sink_config)
        assert isinstance(result, dict)

    def test_incremental_has_more_activities_than_full(self, incremental_source, sink_config):
        meta = ADFPipelineMetadata(
            pipeline_name="pl_ingest_orders_incr",
            source_system="sql_server",
            target_zone="raw",
            load_type="incremental",
        )
        builder = ADFPipelineBuilder(metadata=meta)
        result = builder.build_incremental_pipeline(incremental_source, sink_config)
        activities = result.get("properties", {}).get("activities", [])
        # Incremental adds watermark update activity
        assert len(activities) >= 2

    def test_incremental_raises_without_incremental_column(self, source_config, sink_config):
        # source_config has no incremental_column
        meta = ADFPipelineMetadata(
            pipeline_name="pl_test",
            source_system="sql_server",
            target_zone="raw",
            load_type="incremental",
        )
        builder = ADFPipelineBuilder(metadata=meta)
        with pytest.raises(ValueError, match="incremental_column"):
            builder.build_incremental_pipeline(source_config, sink_config)

    def test_incremental_pipeline_name(self, incremental_source, sink_config):
        meta = ADFPipelineMetadata(
            pipeline_name="pl_ingest_orders_incr",
            source_system="sql_server",
            target_zone="raw",
            load_type="incremental",
        )
        builder = ADFPipelineBuilder(metadata=meta)
        result = builder.build_incremental_pipeline(incremental_source, sink_config)
        assert result["name"] == "pl_ingest_orders_incr"

    def test_incremental_has_watermark_params(self, incremental_source, sink_config):
        meta = ADFPipelineMetadata(
            pipeline_name="pl_ingest_orders_incr",
            source_system="sql_server",
            target_zone="raw",
            load_type="incremental",
        )
        builder = ADFPipelineBuilder(metadata=meta)
        result = builder.build_incremental_pipeline(incremental_source, sink_config)
        params = result.get("properties", {}).get("parameters", {})
        assert "WatermarkColumn" in params or "LastWatermark" in params


# ─────────────────────────────────────────────────────────────────────────────
# ADFOrchestratorPipeline tests
# ─────────────────────────────────────────────────────────────────────────────

class TestADFOrchestratorPipeline:
    def test_orchestrator_builds(self):
        orch = ADFOrchestratorPipeline(name="pl_master_orchestrator")
        orch.add_execute_pipeline("pl_ingest_customers")
        orch.add_execute_pipeline("pl_ingest_orders")
        result = orch.build()
        assert isinstance(result, dict)

    def test_orchestrator_has_execute_pipeline_activities(self):
        orch = ADFOrchestratorPipeline(name="pl_master_orchestrator")
        orch.add_execute_pipeline("pl_ingest_customers")
        result = orch.build()
        activities = result.get("properties", {}).get("activities", [])
        types = [a.get("type") for a in activities]
        assert "ExecutePipeline" in types

    def test_orchestrator_two_child_pipelines(self):
        orch = ADFOrchestratorPipeline(name="pl_master_orchestrator")
        orch.add_execute_pipeline("pl_ingest_customers")
        orch.add_execute_pipeline("pl_ingest_orders")
        result = orch.build()
        activities = result.get("properties", {}).get("activities", [])
        exec_acts = [a for a in activities if a.get("type") == "ExecutePipeline"]
        assert len(exec_acts) == 2

    def test_orchestrator_name_preserved(self):
        orch = ADFOrchestratorPipeline(name="pl_master_orchestrator")
        result = orch.build()
        assert result["name"] == "pl_master_orchestrator"

    def test_add_execute_pipeline_returns_self(self):
        orch = ADFOrchestratorPipeline(name="pl_master")
        result = orch.add_execute_pipeline("pl_child")
        assert result is orch  # chainable

    def test_orchestrator_depends_on(self):
        orch = ADFOrchestratorPipeline(name="pl_master")
        orch.add_execute_pipeline("pl_child_1")
        orch.add_execute_pipeline("pl_child_2", depends_on=["pl_child_1"])
        result = orch.build()
        activities = result.get("properties", {}).get("activities", [])
        child_2 = next(a for a in activities if "pl_child_2" in a.get("name", ""))
        assert len(child_2.get("dependsOn", [])) > 0

    def test_orchestrator_to_json(self):
        orch = ADFOrchestratorPipeline(name="pl_master")
        json_str = orch.to_json()
        parsed = json.loads(json_str)
        assert parsed["name"] == "pl_master"


# ─────────────────────────────────────────────────────────────────────────────
# Utility function tests
# ─────────────────────────────────────────────────────────────────────────────

class TestUtilityFunctions:
    def test_build_standard_ingestion_suite_returns_dict(self):
        suite = build_standard_ingestion_suite(
            source_tables=["Customers", "Orders", "Products"],
            source_server="sql.example.com",
            source_database="OperationsDB",
            storage_account="mysa",
            raw_container="raw",
        )
        assert isinstance(suite, dict)

    def test_build_standard_suite_has_three_pipelines(self):
        suite = build_standard_ingestion_suite(
            source_tables=["Customers", "Orders", "Products"],
            source_server="sql.example.com",
            source_database="OperationsDB",
            storage_account="mysa",
            raw_container="raw",
        )
        assert len(suite) == 3

    def test_build_standard_suite_pipeline_keys_by_name(self):
        suite = build_standard_ingestion_suite(
            source_tables=["Customers"],
            source_server="sql.example.com",
            source_database="OperationsDB",
            storage_account="mysa",
            raw_container="raw",
        )
        keys = list(suite.keys())
        assert any("customers" in k.lower() for k in keys)

    def test_validate_pipeline_structure_valid(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        errors = validate_pipeline_structure(result)
        assert isinstance(errors, list)
        assert len(errors) == 0

    def test_validate_pipeline_structure_missing_name(self):
        bad_pipeline = {"properties": {"activities": []}}
        errors = validate_pipeline_structure(bad_pipeline)
        assert len(errors) > 0

    def test_validate_pipeline_structure_missing_properties(self):
        bad_pipeline = {"name": "test"}
        errors = validate_pipeline_structure(bad_pipeline)
        assert len(errors) > 0

    def test_get_pipeline_metadata_returns_dict(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        meta = get_pipeline_metadata(result)
        assert isinstance(meta, dict)

    def test_get_pipeline_metadata_has_source_system(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        meta = get_pipeline_metadata(result)
        assert "source_system" in meta

    def test_count_activities_by_type(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        counts = count_activities_by_type(result)
        assert isinstance(counts, dict)
        assert "Copy" in counts
        assert counts["Copy"] >= 1

    def test_count_activities_lookup_in_full_pipeline(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        counts = count_activities_by_type(result)
        # Full pipeline should also have a Lookup activity for validation
        assert "Lookup" in counts

    def test_extract_dependencies_returns_dict(self, pipeline_builder, source_config, sink_config):
        result = pipeline_builder.build_full_load_pipeline(source_config, sink_config)
        deps = extract_dependencies(result)
        assert isinstance(deps, dict)

    def test_extract_dependencies_orchestrator(self):
        orch = ADFOrchestratorPipeline(name="pl_master")
        orch.add_execute_pipeline("pl_child_1")
        orch.add_execute_pipeline("pl_child_2", depends_on=["pl_child_1"])
        result = orch.build()
        deps = extract_dependencies(result)
        # Child 2 depends on child 1
        child2_key = [k for k in deps if "pl_child_2" in k]
        assert len(child2_key) > 0
        assert len(deps[child2_key[0]]) > 0
