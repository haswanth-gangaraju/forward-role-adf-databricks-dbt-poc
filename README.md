# ADF + Azure Databricks + dbt Migration Pipeline POC

End-to-end data migration pipeline from SQL Server to Azure Databricks using
Azure Data Factory for orchestration and dbt for transformation modelling.

## Architecture

```
SQL Server (Legacy)
    │
    ▼
Azure Data Factory (ADF)
  ├── Ingestion pipelines (SQL Server → ADLS Gen2)
  ├── Trigger scheduling & dependency management
  └── Pipeline metadata tagging & documentation
    │
    ▼
Azure Data Lake Storage Gen2 (ADLS2) — Raw Zone
    │
    ▼
Azure Databricks — Bronze / Silver Processing
  ├── Bronze: raw ingestion with schema enforcement
  ├── Silver: cleansing, deduplication, reconciliation
  └── Data validation & row-count reconciliation
    │
    ▼
dbt Transformation Layer — Gold / Analytics
  ├── Staging models (source cleaning + renaming)
  ├── Intermediate models (business logic)
  └── Mart models (Power BI-ready fact/dim tables)
    │
    ▼
Power BI / Reporting Layer
```

## Components

| Component | Description |
|-----------|-------------|
| `adf_pipelines/` | ADF pipeline JSON definitions for SQL Server ingestion |
| `databricks_notebooks/` | PySpark Bronze and Silver layer processing |
| `dbt_project/` | dbt staging, intermediate, and mart models with tests |
| `sql_server/` | Source schema DDL and sample data generation |
| `validation/` | Data reconciliation and quality validation framework |
| `tests/` | pytest test suite (160+ assertions) |

## Tech Stack

- **Azure Data Factory** — Pipeline orchestration and scheduling
- **Azure Databricks** — PySpark Bronze/Silver data processing
- **dbt** — Gold layer transformations (staging → intermediate → mart)
- **SQL Server** — Legacy source system
- **ADLS Gen2** — Data lake storage
- **Power BI** — Reporting target (mart layer consumption)
- **GitHub Actions** — CI/CD for dbt model deployment

## Key Features

- **Parameterised ADF pipelines**: Template-based design for onboarding new source tables without pipeline duplication
- **Automated reconciliation**: Source-to-target row count, checksum, and null-distribution checks
- **dbt test coverage**: Schema, uniqueness, referential integrity, and custom business rule tests
- **Pipeline documentation**: ADF metadata tagging + dbt docs generation

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Project Structure

```
forward-role-adf-databricks-dbt-poc/
├── adf_pipelines/
│   ├── pipeline_sql_to_adls.json      # SQL Server → ADLS2 copy activity
│   ├── pipeline_orchestrator.json     # Master orchestration pipeline
│   └── pipeline_factory.py            # Pipeline JSON generation utilities
├── databricks_notebooks/
│   ├── bronze_ingest.py               # Raw ingestion with schema enforcement
│   └── silver_transform.py            # Cleansing, deduplication, reconciliation
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                   # Source cleaning (stg_* models)
│   │   ├── intermediate/              # Business logic (int_* models)
│   │   └── mart/                      # Analytics-ready (fct_*, dim_* models)
│   └── tests/                         # Custom dbt test definitions
├── sql_server/
│   └── source_schema.py               # Source DDL and sample data utilities
├── validation/
│   └── reconciliation.py              # Source-to-target reconciliation framework
├── tests/
│   ├── test_adf_pipelines.py
│   ├── test_databricks_notebooks.py
│   ├── test_dbt_models.py
│   └── test_reconciliation.py
├── requirements.txt
└── README.md
```
