"""
ADF Pipeline Factory
Generates parameterised Azure Data Factory pipeline JSON definitions
for SQL Server → ADLS Gen2 ingestion patterns.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ADFSourceConfig:
    """Configuration for a SQL Server source."""
    server: str
    database: str
    table: str
    schema: str = "dbo"
    incremental_column: Optional[str] = None
    watermark_value: Optional[str] = None


@dataclass
class ADFSinkConfig:
    """Configuration for an ADLS Gen2 sink."""
    storage_account: str
    container: str
    folder_path: str
    file_format: str = "parquet"     # parquet | delta | csv
    partition_by: Optional[str] = None


@dataclass
class ADFPipelineMetadata:
    """Pipeline metadata for documentation and lineage tracking."""
    pipeline_name: str
    source_system: str
    target_zone: str                  # raw | bronze | silver | gold
    load_type: str                    # full | incremental | delta
    owner: str = "data-engineering"
    tags: List[str] = field(default_factory=list)
    description: str = ""


class ADFPipelineBuilder:
    """
    Builds ADF pipeline JSON definitions programmatically.
    Supports full-load and incremental copy patterns for SQL Server → ADLS.
    """

    def __init__(self, metadata: ADFPipelineMetadata):
        self.metadata = metadata

    def _build_copy_activity(
        self,
        source: ADFSourceConfig,
        sink: ADFSinkConfig,
        activity_name: str = "CopyFromSQLToADLS"
    ) -> Dict[str, Any]:
        """Generate a CopyActivity definition."""
        source_dataset = f"DS_{source.schema}_{source.table}_SQL"
        sink_dataset   = f"DS_{sink.folder_path.replace('/', '_')}_{sink.file_format.upper()}"

        activity: Dict[str, Any] = {
            "name": activity_name,
            "type": "Copy",
            "dependsOn": [],
            "policy": {
                "timeout": "0.12:00:00",
                "retry": 3,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "SqlServerSource",
                    "sqlReaderQuery": self._build_source_query(source),
                    "queryTimeout": "02:00:00",
                    "partitionOption": "None"
                },
                "sink": {
                    "type": "ParquetSink" if sink.file_format == "parquet" else "DelimitedTextSink",
                    "storeSettings": {
                        "type": "AzureBlobFSWriteSettings"
                    },
                    "formatSettings": {
                        "type": "ParquetWriteSettings" if sink.file_format == "parquet" else "DelimitedTextWriteSettings"
                    }
                },
                "enableStaging": False,
                "translator": {"type": "TabularTranslator", "typeConversion": True}
            },
            "inputs": [{"referenceName": source_dataset, "type": "DatasetReference"}],
            "outputs": [{"referenceName": sink_dataset, "type": "DatasetReference"}]
        }

        return activity

    def _build_source_query(self, source: ADFSourceConfig) -> str:
        """Build the SQL query for the source dataset."""
        base_query = f"SELECT * FROM [{source.schema}].[{source.table}]"
        if source.incremental_column and source.watermark_value:
            base_query += f" WHERE [{source.incremental_column}] > '{source.watermark_value}'"
        return base_query

    def _build_set_watermark_activity(
        self,
        source: ADFSourceConfig,
        activity_name: str = "SetWatermark"
    ) -> Dict[str, Any]:
        """Generate a watermark update stored procedure activity."""
        return {
            "name": activity_name,
            "type": "SqlServerStoredProcedure",
            "dependsOn": [{"activity": "CopyFromSQLToADLS", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {
                "storedProcedureName": "usp_update_watermark",
                "storedProcedureParameters": {
                    "TableName": {"value": f"{source.schema}.{source.table}", "type": "String"},
                    "NewWatermarkValue": {
                        "value": "@activity('CopyFromSQLToADLS').output.dataRead",
                        "type": "DateTime"
                    }
                }
            }
        }

    def _build_validation_activity(self, activity_name: str = "ValidateRowCounts") -> Dict[str, Any]:
        """Generate a row-count reconciliation validation activity."""
        return {
            "name": activity_name,
            "type": "Lookup",
            "dependsOn": [{"activity": "CopyFromSQLToADLS", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {
                "source": {
                    "type": "SqlServerSource",
                    "sqlReaderQuery": "SELECT COUNT(*) AS source_count FROM @{pipeline().parameters.SourceTable}"
                },
                "dataset": {"referenceName": "DS_Validation_SQL", "type": "DatasetReference"},
                "firstRowOnly": True
            }
        }

    def build_full_load_pipeline(
        self,
        source: ADFSourceConfig,
        sink: ADFSinkConfig
    ) -> Dict[str, Any]:
        """Build a full-load pipeline definition."""
        activities = [
            self._build_copy_activity(source, sink),
            self._build_validation_activity()
        ]

        return {
            "name": self.metadata.pipeline_name,
            "properties": {
                "description": self.metadata.description or f"Full load: {source.schema}.{source.table} → {sink.folder_path}",
                "activities": activities,
                "parameters": {
                    "SourceTable": {"type": "String", "defaultValue": f"{source.schema}.{source.table}"},
                    "TargetPath": {"type": "String", "defaultValue": sink.folder_path}
                },
                "annotations": [
                    f"source_system:{self.metadata.source_system}",
                    f"target_zone:{self.metadata.target_zone}",
                    f"load_type:{self.metadata.load_type}",
                    f"owner:{self.metadata.owner}",
                ] + [f"tag:{t}" for t in self.metadata.tags]
            },
            "type": "Microsoft.DataFactory/factories/pipelines"
        }

    def build_incremental_pipeline(
        self,
        source: ADFSourceConfig,
        sink: ADFSinkConfig
    ) -> Dict[str, Any]:
        """Build an incremental-load pipeline definition."""
        if not source.incremental_column:
            raise ValueError("incremental_column is required for incremental pipelines")

        activities = [
            self._build_copy_activity(source, sink),
            self._build_set_watermark_activity(source),
            self._build_validation_activity()
        ]

        return {
            "name": self.metadata.pipeline_name,
            "properties": {
                "description": self.metadata.description or f"Incremental load: {source.schema}.{source.table} by {source.incremental_column}",
                "activities": activities,
                "parameters": {
                    "SourceTable": {"type": "String", "defaultValue": f"{source.schema}.{source.table}"},
                    "WatermarkColumn": {"type": "String", "defaultValue": source.incremental_column},
                    "LastWatermark": {"type": "String", "defaultValue": source.watermark_value or "1900-01-01"}
                },
                "annotations": [
                    f"source_system:{self.metadata.source_system}",
                    f"load_type:incremental",
                    f"watermark_column:{source.incremental_column}",
                    f"owner:{self.metadata.owner}"
                ]
            },
            "type": "Microsoft.DataFactory/factories/pipelines"
        }

    def to_json(self, pipeline_def: Dict[str, Any]) -> str:
        """Serialise pipeline definition to JSON string."""
        return json.dumps(pipeline_def, indent=2)


class ADFOrchestratorPipeline:
    """
    Builds a master orchestration pipeline that executes child pipelines
    in dependency order with error handling.
    """

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._activities: List[Dict[str, Any]] = []

    def add_execute_pipeline(
        self,
        pipeline_name: str,
        depends_on: Optional[List[str]] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> "ADFOrchestratorPipeline":
        """Add an ExecutePipeline activity."""
        depends = []
        if depends_on:
            for dep in depends_on:
                depends.append({"activity": dep, "dependencyConditions": ["Succeeded"]})

        activity: Dict[str, Any] = {
            "name": f"Execute_{pipeline_name}",
            "type": "ExecutePipeline",
            "dependsOn": depends,
            "typeProperties": {
                "pipeline": {"referenceName": pipeline_name, "type": "PipelineReference"},
                "waitOnCompletion": True
            }
        }
        if parameters:
            activity["typeProperties"]["parameters"] = parameters

        self._activities.append(activity)
        return self

    def build(self) -> Dict[str, Any]:
        """Build the orchestrator pipeline definition."""
        return {
            "name": self.name,
            "properties": {
                "description": self.description,
                "activities": self._activities,
                "annotations": ["type:orchestrator", "layer:master"]
            },
            "type": "Microsoft.DataFactory/factories/pipelines"
        }

    def to_json(self) -> str:
        return json.dumps(self.build(), indent=2)


def build_standard_ingestion_suite(
    source_tables: List[str],
    source_server: str,
    source_database: str,
    storage_account: str,
    raw_container: str
) -> Dict[str, Dict[str, Any]]:
    """
    Build a standard suite of ingestion pipelines for a list of source tables.
    Returns a dict of pipeline_name → pipeline_definition.
    """
    pipelines: Dict[str, Dict[str, Any]] = {}

    for table in source_tables:
        pipeline_name = f"pl_ingest_{table.lower()}"
        source = ADFSourceConfig(
            server=source_server,
            database=source_database,
            table=table,
            incremental_column="updated_at",
            watermark_value="1900-01-01 00:00:00"
        )
        sink = ADFSinkConfig(
            storage_account=storage_account,
            container=raw_container,
            folder_path=f"raw/{source_database}/{table}",
            file_format="parquet",
            partition_by="ingestion_date"
        )
        metadata = ADFPipelineMetadata(
            pipeline_name=pipeline_name,
            source_system="sql_server",
            target_zone="raw",
            load_type="incremental",
            owner="data-engineering",
            tags=["sql-server-migration", "automated"],
            description=f"Incremental ingestion of {source_database}.{table}"
        )
        builder = ADFPipelineBuilder(metadata)
        pipelines[pipeline_name] = builder.build_incremental_pipeline(source, sink)

    return pipelines


def validate_pipeline_structure(pipeline_def: Dict[str, Any]) -> List[str]:
    """
    Validate that a pipeline definition has the required structural components.
    Returns a list of validation errors (empty if valid).
    """
    errors: List[str] = []

    if "name" not in pipeline_def:
        errors.append("Pipeline missing 'name' field")

    if "properties" not in pipeline_def:
        errors.append("Pipeline missing 'properties' field")
        return errors

    props = pipeline_def["properties"]

    if "activities" not in props:
        errors.append("Pipeline missing 'activities' field")
    elif not isinstance(props["activities"], list):
        errors.append("'activities' must be a list")
    else:
        for i, activity in enumerate(props["activities"]):
            if "name" not in activity:
                errors.append(f"Activity[{i}] missing 'name'")
            if "type" not in activity:
                errors.append(f"Activity[{i}] missing 'type'")
            if "typeProperties" not in activity:
                errors.append(f"Activity[{i}] '{activity.get('name', '?')}' missing 'typeProperties'")

    return errors


def get_pipeline_metadata(pipeline_def: Dict[str, Any]) -> Dict[str, str]:
    """Extract lineage metadata tags from a pipeline definition."""
    metadata: Dict[str, str] = {}
    annotations = pipeline_def.get("properties", {}).get("annotations", [])
    for annotation in annotations:
        if ":" in annotation:
            key, value = annotation.split(":", 1)
            metadata[key] = value
    return metadata


def count_activities_by_type(pipeline_def: Dict[str, Any]) -> Dict[str, int]:
    """Count activities grouped by type."""
    counts: Dict[str, int] = {}
    activities = pipeline_def.get("properties", {}).get("activities", [])
    for activity in activities:
        activity_type = activity.get("type", "Unknown")
        counts[activity_type] = counts.get(activity_type, 0) + 1
    return counts


def extract_dependencies(pipeline_def: Dict[str, Any]) -> Dict[str, List[str]]:
    """Extract dependency graph from a pipeline definition."""
    deps: Dict[str, List[str]] = {}
    activities = pipeline_def.get("properties", {}).get("activities", [])
    for activity in activities:
        name = activity.get("name", "")
        depends_on = [d["activity"] for d in activity.get("dependsOn", [])]
        deps[name] = depends_on
    return deps
