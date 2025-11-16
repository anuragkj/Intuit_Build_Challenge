"""
Unit Tests for Data Loader Module.

Tests CSV parsing, data validation, and error handling.
"""

import tempfile
from decimal import Decimal
from pathlib import Path

import pytest

from src.data_loader import (
    SalesTransaction,
    load_csv,
    load_csv_as_list,
    parse_decimal,
    parse_float,
    parse_int,
    parse_optional_string,
    parse_row,
    validate_transaction,
)


def test_parse_decimal_valid() -> None:
    """Test parsing valid decimal strings."""
    assert parse_decimal("123.45") == Decimal("123.45")
    assert parse_decimal("0.99") == Decimal("0.99")
    assert parse_decimal("1000") == Decimal("1000")


def test_parse_decimal_invalid() -> None:
    """Test parsing invalid decimal strings raises ValueError."""
    with pytest.raises(ValueError, match="Cannot parse"):
        parse_decimal("not a number")


def test_parse_int_valid() -> None:
    """Test parsing valid integer strings."""
    assert parse_int("42") == 42
    assert parse_int("0") == 0
    assert parse_int("-10") == -10


def test_parse_int_invalid() -> None:
    """Test parsing invalid integer strings raises ValueError."""
    with pytest.raises(ValueError, match="Cannot parse"):
        parse_int("3.14")


def test_parse_float_valid() -> None:
    """Test parsing valid float strings."""
    assert parse_float("3.14") == 3.14
    assert parse_float("0.0") == 0.0
    assert parse_float("100") == 100.0


def test_parse_float_invalid() -> None:
    """Test parsing invalid float strings raises ValueError."""
    with pytest.raises(ValueError, match="Cannot parse"):
        parse_float("not a number")


def test_parse_optional_string() -> None:
    """Test parsing optional strings (handles empty)."""
    assert parse_optional_string("value") == "value"
    assert parse_optional_string("  spaced  ") == "spaced"
    assert parse_optional_string("") is None
    assert parse_optional_string("   ") is None


def test_parse_row_valid() -> None:
    """Test parsing valid CSV row."""
    row = {
        "transaction_id": "TXN-0000001",
        "date": "2023-01-01",
        "timestamp": "2023-01-01 12:00:00",
        "customer_id": "CUST-00001",
        "product_id": "PROD-0001",
        "product_category": "Electronics",
        "product_name": "Laptop",
        "quantity": "2",
        "unit_price": "999.99",
        "total_amount": "1999.98",
        "discount_percent": "0.0",
        "payment_method": "Credit Card",
        "region": "North",
        "sales_rep_id": "REP-001",
        "customer_segment": "Enterprise",
    }

    transaction = parse_row(row)

    assert transaction.transaction_id == "TXN-0000001"
    assert transaction.product_category == "Electronics"
    assert transaction.quantity == 2
    assert transaction.unit_price == Decimal("999.99")
    assert transaction.total_amount == Decimal("1999.98")
    assert transaction.discount_percent == 0.0


def test_parse_row_with_empty_customer() -> None:
    """Test parsing row with empty customer_id."""
    row = {
        "transaction_id": "TXN-0000001",
        "date": "2023-01-01",
        "timestamp": "2023-01-01 12:00:00",
        "customer_id": "",
        "product_id": "PROD-0001",
        "product_category": "Electronics",
        "product_name": "Laptop",
        "quantity": "1",
        "unit_price": "999.99",
        "total_amount": "999.99",
        "discount_percent": "0.0",
        "payment_method": "Cash",
        "region": "North",
        "sales_rep_id": "",
        "customer_segment": "Individual",
    }

    transaction = parse_row(row)
    assert transaction.customer_id is None
    assert transaction.sales_rep_id is None


def test_parse_row_missing_column() -> None:
    """Test parsing row with missing required column."""
    row = {
        "transaction_id": "TXN-0000001",
        "date": "2023-01-01",
        # Missing other columns
    }

    with pytest.raises(ValueError, match="Missing required column"):
        parse_row(row)


def test_parse_row_invalid_numeric() -> None:
    """Test parsing row with invalid numeric value."""
    row = {
        "transaction_id": "TXN-0000001",
        "date": "2023-01-01",
        "timestamp": "2023-01-01 12:00:00",
        "customer_id": "CUST-00001",
        "product_id": "PROD-0001",
        "product_category": "Electronics",
        "product_name": "Laptop",
        "quantity": "not a number",
        "unit_price": "999.99",
        "total_amount": "1999.98",
        "discount_percent": "0.0",
        "payment_method": "Credit Card",
        "region": "North",
        "sales_rep_id": "REP-001",
        "customer_segment": "Enterprise",
    }

    with pytest.raises(ValueError):
        parse_row(row)


def test_validate_transaction_valid() -> None:
    """Test validation of valid transaction."""
    transaction = SalesTransaction(
        transaction_id="TXN-0000001",
        date="2023-01-01",
        timestamp="2023-01-01 12:00:00",
        customer_id="CUST-00001",
        product_id="PROD-0001",
        product_category="Electronics",
        product_name="Laptop",
        quantity=2,
        unit_price=Decimal("999.99"),
        total_amount=Decimal("1999.98"),
        discount_percent=0.0,
        payment_method="Credit Card",
        region="North",
        sales_rep_id="REP-001",
        customer_segment="Enterprise",
    )

    assert validate_transaction(transaction) is True


def test_validate_transaction_invalid_quantity() -> None:
    """Test validation fails for zero/negative quantity."""
    transaction = SalesTransaction(
        transaction_id="TXN-0000001",
        date="2023-01-01",
        timestamp="2023-01-01 12:00:00",
        customer_id="CUST-00001",
        product_id="PROD-0001",
        product_category="Electronics",
        product_name="Laptop",
        quantity=0,
        unit_price=Decimal("999.99"),
        total_amount=Decimal("0"),
        discount_percent=0.0,
        payment_method="Credit Card",
        region="North",
        sales_rep_id="REP-001",
        customer_segment="Enterprise",
    )

    assert validate_transaction(transaction) is False


def test_validate_transaction_invalid_discount() -> None:
    """Test validation fails for invalid discount percent."""
    transaction = SalesTransaction(
        transaction_id="TXN-0000001",
        date="2023-01-01",
        timestamp="2023-01-01 12:00:00",
        customer_id="CUST-00001",
        product_id="PROD-0001",
        product_category="Electronics",
        product_name="Laptop",
        quantity=1,
        unit_price=Decimal("999.99"),
        total_amount=Decimal("999.99"),
        discount_percent=150.0,  # Invalid: > 100
        payment_method="Credit Card",
        region="North",
        sales_rep_id="REP-001",
        customer_segment="Enterprise",
    )

    assert validate_transaction(transaction) is False


def test_load_csv_file_not_found() -> None:
    """Test loading non-existent CSV raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        list(load_csv("nonexistent.csv"))


def test_load_csv_valid_file() -> None:
    """Test loading valid CSV file."""
    # Create temporary CSV file
    csv_content = """transaction_id,date,timestamp,customer_id,product_id,product_category,product_name,quantity,unit_price,total_amount,discount_percent,payment_method,region,sales_rep_id,customer_segment
TXN-0000001,2023-01-01,2023-01-01 12:00:00,CUST-00001,PROD-0001,Electronics,Laptop,2,999.99,1999.98,0.0,Credit Card,North,REP-001,Enterprise
TXN-0000002,2023-01-02,2023-01-02 13:00:00,CUST-00002,PROD-0002,Books,Novel,1,25.00,25.00,10.0,Cash,South,REP-002,Individual
"""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(csv_content)
        temp_path = f.name

    try:
        transactions = load_csv_as_list(temp_path)

        assert len(transactions) == 2
        assert transactions[0].transaction_id == "TXN-0000001"
        assert transactions[0].product_category == "Electronics"
        assert transactions[1].transaction_id == "TXN-0000002"
        assert transactions[1].discount_percent == 10.0
    finally:
        Path(temp_path).unlink()


def test_load_csv_empty_file() -> None:
    """Test loading CSV with only header."""
    csv_content = """transaction_id,date,timestamp,customer_id,product_id,product_category,product_name,quantity,unit_price,total_amount,discount_percent,payment_method,region,sales_rep_id,customer_segment
"""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(csv_content)
        temp_path = f.name

    try:
        transactions = load_csv_as_list(temp_path)
        assert len(transactions) == 0
    finally:
        Path(temp_path).unlink()


def test_sales_transaction_immutable() -> None:
    """Test that SalesTransaction is immutable (frozen dataclass)."""
    transaction = SalesTransaction(
        transaction_id="TXN-0000001",
        date="2023-01-01",
        timestamp="2023-01-01 12:00:00",
        customer_id="CUST-00001",
        product_id="PROD-0001",
        product_category="Electronics",
        product_name="Laptop",
        quantity=2,
        unit_price=Decimal("999.99"),
        total_amount=Decimal("1999.98"),
        discount_percent=0.0,
        payment_method="Credit Card",
        region="North",
        sales_rep_id="REP-001",
        customer_segment="Enterprise",
    )

    # Attempting to modify should raise exception
    with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
        transaction.quantity = 5  # type: ignore
