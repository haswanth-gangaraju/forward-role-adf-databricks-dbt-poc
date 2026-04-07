"""
SQL Server Source Schema Definitions and Sample Data Generator.
Provides source table DDL representations and test data for validation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
import random


# ─────────────────────────────────────────────────────────────────────────────
# Source schema definitions
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SQLColumn:
    name: str
    sql_type: str
    nullable: bool = True
    primary_key: bool = False
    identity: bool = False
    default: Optional[str] = None


@dataclass
class SQLTable:
    schema: str
    table_name: str
    columns: List[SQLColumn] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"[{self.schema}].[{self.table_name}]"

    def primary_keys(self) -> List[str]:
        return [c.name for c in self.columns if c.primary_key]

    def to_create_ddl(self) -> str:
        col_defs = []
        pk_cols = []
        for col in self.columns:
            nullable = "NULL" if col.nullable else "NOT NULL"
            identity = " IDENTITY(1,1)" if col.identity else ""
            default = f" DEFAULT {col.default}" if col.default else ""
            col_defs.append(f"    [{col.name}] {col.sql_type}{identity} {nullable}{default}")
            if col.primary_key:
                pk_cols.append(f"[{col.name}]")

        ddl = f"CREATE TABLE {self.full_name} (\n"
        ddl += ",\n".join(col_defs)
        if pk_cols:
            pk_name = f"PK_{self.table_name}"
            ddl += f",\n    CONSTRAINT [{pk_name}] PRIMARY KEY ({', '.join(pk_cols)})"
        ddl += "\n);\n"
        return ddl


# Source table definitions
CUSTOMERS_TABLE = SQLTable(
    schema="dbo",
    table_name="Customers",
    columns=[
        SQLColumn("CustomerID", "INT", nullable=False, primary_key=True, identity=True),
        SQLColumn("CustomerName", "NVARCHAR(200)", nullable=False),
        SQLColumn("CustomerEmail", "NVARCHAR(200)", nullable=True),
        SQLColumn("CustomerStatus", "NVARCHAR(50)", nullable=False, default="'ACTIVE'"),
        SQLColumn("CreatedDate", "DATETIME", nullable=False, default="GETDATE()"),
        SQLColumn("UpdatedDate", "DATETIME", nullable=True),
        SQLColumn("RegionCode", "NVARCHAR(10)", nullable=True),
        SQLColumn("AccountManagerID", "INT", nullable=True),
    ]
)

ORDERS_TABLE = SQLTable(
    schema="dbo",
    table_name="Orders",
    columns=[
        SQLColumn("OrderID", "INT", nullable=False, primary_key=True, identity=True),
        SQLColumn("CustomerID", "INT", nullable=False),
        SQLColumn("ProductID", "INT", nullable=False),
        SQLColumn("OrderDate", "DATE", nullable=False),
        SQLColumn("ShipDate", "DATE", nullable=True),
        SQLColumn("OrderStatus", "NVARCHAR(50)", nullable=False, default="'PENDING'"),
        SQLColumn("TotalAmount", "DECIMAL(18,2)", nullable=False),
        SQLColumn("CurrencyCode", "NCHAR(3)", nullable=False, default="'GBP'"),
        SQLColumn("SalesRepID", "INT", nullable=True),
        SQLColumn("ChannelCode", "NVARCHAR(20)", nullable=True),
    ]
)

PRODUCTS_TABLE = SQLTable(
    schema="dbo",
    table_name="Products",
    columns=[
        SQLColumn("ProductID", "INT", nullable=False, primary_key=True, identity=True),
        SQLColumn("ProductName", "NVARCHAR(200)", nullable=False),
        SQLColumn("ProductCode", "NVARCHAR(50)", nullable=False),
        SQLColumn("CategoryID", "INT", nullable=True),
        SQLColumn("UnitPrice", "DECIMAL(18,2)", nullable=False),
        SQLColumn("StockQuantity", "INT", nullable=False, default="0"),
        SQLColumn("IsActive", "BIT", nullable=False, default="1"),
        SQLColumn("CreatedDate", "DATETIME", nullable=False, default="GETDATE()"),
    ]
)

ALL_SOURCE_TABLES = [CUSTOMERS_TABLE, ORDERS_TABLE, PRODUCTS_TABLE]


# ─────────────────────────────────────────────────────────────────────────────
# Sample data generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_sample_customers(count: int = 10, seed: int = 42) -> List[Dict[str, Any]]:
    """Generate sample customer records matching the SQL Server schema."""
    rng = random.Random(seed)
    statuses = ["ACTIVE", "INACTIVE", "SUSPENDED", "PENDING"]
    regions = ["NORTH", "SOUTH", "EAST", "WEST", "MIDLANDS", "SCOTLAND"]
    names = [
        "Acme Corp", "Barton Ltd", "Clarke Manufacturing", "Devlin Logistics",
        "Evans Healthcare", "Foster Technologies", "Grant & Co", "Harris Group",
        "Irving Solutions", "Jenkins Retail"
    ]

    records = []
    base_date = date(2022, 1, 1)
    for i in range(1, count + 1):
        created = base_date + timedelta(days=rng.randint(0, 365))
        records.append({
            "CustomerID": i,
            "CustomerName": names[i % len(names)] + (f" {i}" if i > len(names) else ""),
            "CustomerEmail": f"contact{i}@company{i}.co.uk",
            "CustomerStatus": rng.choice(statuses),
            "CreatedDate": datetime(created.year, created.month, created.day),
            "UpdatedDate": None if rng.random() < 0.3 else datetime(2024, rng.randint(1, 12), rng.randint(1, 28)),
            "RegionCode": rng.choice(regions),
            "AccountManagerID": rng.randint(1, 5),
        })
    return records


def generate_sample_orders(
    customer_ids: List[int],
    product_ids: List[int],
    count: int = 20,
    seed: int = 42
) -> List[Dict[str, Any]]:
    """Generate sample order records."""
    rng = random.Random(seed)
    statuses = ["COMPLETED", "PENDING", "SHIPPED", "CANCELLED", "RETURNED"]
    channels = ["ONLINE", "PHONE", "DIRECT", "PARTNER"]

    records = []
    base_date = date(2023, 1, 1)
    for i in range(1, count + 1):
        order_date = base_date + timedelta(days=rng.randint(0, 365))
        ship_date = order_date + timedelta(days=rng.randint(1, 7)) if rng.random() > 0.2 else None
        records.append({
            "OrderID": i,
            "CustomerID": rng.choice(customer_ids),
            "ProductID": rng.choice(product_ids),
            "OrderDate": order_date,
            "ShipDate": ship_date,
            "OrderStatus": rng.choice(statuses),
            "TotalAmount": round(rng.uniform(50.0, 5000.0), 2),
            "CurrencyCode": "GBP",
            "SalesRepID": rng.randint(1, 10),
            "ChannelCode": rng.choice(channels),
        })
    return records


def generate_sample_products(count: int = 5, seed: int = 42) -> List[Dict[str, Any]]:
    """Generate sample product records."""
    rng = random.Random(seed)
    categories = [1, 2, 3, 4, 5]
    product_names = [
        "Enterprise Suite", "Analytics Pro", "Cloud Connector",
        "Data Vault License", "Migration Toolkit",
        "Reporting Dashboard", "API Gateway", "ETL Accelerator"
    ]

    records = []
    for i in range(1, count + 1):
        records.append({
            "ProductID": i,
            "ProductName": product_names[i % len(product_names)],
            "ProductCode": f"PROD-{i:04d}",
            "CategoryID": rng.choice(categories),
            "UnitPrice": round(rng.uniform(100.0, 10000.0), 2),
            "StockQuantity": rng.randint(0, 500),
            "IsActive": rng.random() > 0.1,
            "CreatedDate": datetime(2021, rng.randint(1, 12), rng.randint(1, 28)),
        })
    return records


def get_all_source_ddl() -> str:
    """Return CREATE TABLE DDL for all source tables."""
    return "\n".join(table.to_create_ddl() for table in ALL_SOURCE_TABLES)
