"""
Tests for SQL Server source schema definitions and sample data generators.
"""
import pytest
from datetime import datetime, date
from sql_server.source_schema import (
    SQLColumn,
    SQLTable,
    CUSTOMERS_TABLE,
    ORDERS_TABLE,
    PRODUCTS_TABLE,
    ALL_SOURCE_TABLES,
    generate_sample_customers,
    generate_sample_orders,
    generate_sample_products,
    get_all_source_ddl,
)


# ─────────────────────────────────────────────────────────────────────────────
# SQLColumn tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSQLColumn:
    def test_basic_column(self):
        col = SQLColumn("CustomerID", "INT", nullable=False, primary_key=True)
        assert col.name == "CustomerID"
        assert col.sql_type == "INT"
        assert col.nullable is False
        assert col.primary_key is True

    def test_defaults(self):
        col = SQLColumn("Name", "NVARCHAR(200)")
        assert col.nullable is True
        assert col.primary_key is False
        assert col.identity is False
        assert col.default is None

    def test_identity_column(self):
        col = SQLColumn("ID", "INT", nullable=False, identity=True)
        assert col.identity is True


# ─────────────────────────────────────────────────────────────────────────────
# SQLTable tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSQLTable:
    def test_full_name(self):
        assert CUSTOMERS_TABLE.full_name == "[dbo].[Customers]"

    def test_orders_full_name(self):
        assert ORDERS_TABLE.full_name == "[dbo].[Orders]"

    def test_primary_keys_customers(self):
        pks = CUSTOMERS_TABLE.primary_keys()
        assert "CustomerID" in pks
        assert len(pks) == 1

    def test_primary_keys_orders(self):
        pks = ORDERS_TABLE.primary_keys()
        assert "OrderID" in pks

    def test_primary_keys_products(self):
        pks = PRODUCTS_TABLE.primary_keys()
        assert "ProductID" in pks

    def test_ddl_contains_create_table(self):
        ddl = CUSTOMERS_TABLE.to_create_ddl()
        assert "CREATE TABLE" in ddl

    def test_ddl_contains_column_names(self):
        ddl = CUSTOMERS_TABLE.to_create_ddl()
        assert "CustomerID" in ddl
        assert "CustomerName" in ddl
        assert "CustomerEmail" in ddl

    def test_ddl_contains_primary_key_constraint(self):
        ddl = CUSTOMERS_TABLE.to_create_ddl()
        assert "PRIMARY KEY" in ddl

    def test_ddl_identity_syntax(self):
        ddl = CUSTOMERS_TABLE.to_create_ddl()
        assert "IDENTITY" in ddl

    def test_ddl_default_values(self):
        ddl = CUSTOMERS_TABLE.to_create_ddl()
        assert "DEFAULT" in ddl

    def test_orders_table_columns(self):
        col_names = [c.name for c in ORDERS_TABLE.columns]
        assert "OrderID" in col_names
        assert "CustomerID" in col_names
        assert "TotalAmount" in col_names
        assert "CurrencyCode" in col_names


# ─────────────────────────────────────────────────────────────────────────────
# ALL_SOURCE_TABLES
# ─────────────────────────────────────────────────────────────────────────────

class TestAllSourceTables:
    def test_three_tables(self):
        assert len(ALL_SOURCE_TABLES) == 3

    def test_contains_all_tables(self):
        names = [t.table_name for t in ALL_SOURCE_TABLES]
        assert "Customers" in names
        assert "Orders" in names
        assert "Products" in names


# ─────────────────────────────────────────────────────────────────────────────
# Sample data generators
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerateSampleCustomers:
    def test_default_count(self):
        customers = generate_sample_customers()
        assert len(customers) == 10

    def test_custom_count(self):
        customers = generate_sample_customers(count=5)
        assert len(customers) == 5

    def test_has_required_fields(self):
        customers = generate_sample_customers(count=3)
        for c in customers:
            assert "CustomerID" in c
            assert "CustomerName" in c
            assert "CustomerStatus" in c
            assert "CreatedDate" in c

    def test_customer_id_sequential(self):
        customers = generate_sample_customers(count=5)
        ids = [c["CustomerID"] for c in customers]
        assert ids == list(range(1, 6))

    def test_deterministic_with_seed(self):
        c1 = generate_sample_customers(count=5, seed=99)
        c2 = generate_sample_customers(count=5, seed=99)
        assert c1 == c2

    def test_different_seeds_different_data(self):
        c1 = generate_sample_customers(count=5, seed=1)
        c2 = generate_sample_customers(count=5, seed=2)
        statuses = [(r["CustomerStatus"]) for r in c1]
        statuses2 = [(r["CustomerStatus"]) for r in c2]
        # At least one difference expected with different seeds
        assert c1 != c2 or statuses != statuses2

    def test_created_date_is_datetime(self):
        customers = generate_sample_customers(count=3)
        for c in customers:
            assert isinstance(c["CreatedDate"], datetime)

    def test_valid_status_values(self):
        valid_statuses = {"ACTIVE", "INACTIVE", "SUSPENDED", "PENDING"}
        customers = generate_sample_customers(count=10)
        for c in customers:
            assert c["CustomerStatus"] in valid_statuses

    def test_valid_region_codes(self):
        valid_regions = {"NORTH", "SOUTH", "EAST", "WEST", "MIDLANDS", "SCOTLAND"}
        customers = generate_sample_customers(count=10)
        for c in customers:
            assert c["RegionCode"] in valid_regions


class TestGenerateSampleOrders:
    @pytest.fixture
    def customer_ids(self):
        return list(range(1, 6))

    @pytest.fixture
    def product_ids(self):
        return list(range(1, 4))

    def test_default_count(self, customer_ids, product_ids):
        orders = generate_sample_orders(customer_ids, product_ids)
        assert len(orders) == 20

    def test_custom_count(self, customer_ids, product_ids):
        orders = generate_sample_orders(customer_ids, product_ids, count=10)
        assert len(orders) == 10

    def test_has_required_fields(self, customer_ids, product_ids):
        orders = generate_sample_orders(customer_ids, product_ids, count=5)
        for o in orders:
            assert "OrderID" in o
            assert "CustomerID" in o
            assert "TotalAmount" in o
            assert "OrderStatus" in o
            assert "OrderDate" in o

    def test_customer_ids_from_pool(self, customer_ids, product_ids):
        orders = generate_sample_orders(customer_ids, product_ids, count=20)
        for o in orders:
            assert o["CustomerID"] in customer_ids

    def test_total_amount_positive(self, customer_ids, product_ids):
        orders = generate_sample_orders(customer_ids, product_ids, count=20)
        for o in orders:
            assert o["TotalAmount"] > 0

    def test_currency_is_gbp(self, customer_ids, product_ids):
        orders = generate_sample_orders(customer_ids, product_ids, count=10)
        for o in orders:
            assert o["CurrencyCode"] == "GBP"

    def test_order_date_is_date(self, customer_ids, product_ids):
        orders = generate_sample_orders(customer_ids, product_ids, count=5)
        for o in orders:
            assert isinstance(o["OrderDate"], date)


class TestGenerateSampleProducts:
    def test_default_count(self):
        products = generate_sample_products()
        assert len(products) == 5

    def test_custom_count(self):
        products = generate_sample_products(count=8)
        assert len(products) == 8

    def test_has_required_fields(self):
        products = generate_sample_products(count=3)
        for p in products:
            assert "ProductID" in p
            assert "ProductName" in p
            assert "ProductCode" in p
            assert "UnitPrice" in p

    def test_product_code_format(self):
        products = generate_sample_products(count=5)
        for p in products:
            assert p["ProductCode"].startswith("PROD-")

    def test_unit_price_positive(self):
        products = generate_sample_products(count=5)
        for p in products:
            assert p["UnitPrice"] > 0

    def test_is_active_is_bool(self):
        products = generate_sample_products(count=5)
        for p in products:
            assert isinstance(p["IsActive"], bool)


# ─────────────────────────────────────────────────────────────────────────────
# get_all_source_ddl
# ─────────────────────────────────────────────────────────────────────────────

class TestGetAllSourceDDL:
    def test_returns_string(self):
        ddl = get_all_source_ddl()
        assert isinstance(ddl, str)

    def test_contains_all_tables(self):
        ddl = get_all_source_ddl()
        assert "Customers" in ddl
        assert "Orders" in ddl
        assert "Products" in ddl

    def test_multiple_create_statements(self):
        ddl = get_all_source_ddl()
        count = ddl.count("CREATE TABLE")
        assert count == 3
