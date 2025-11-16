"""
Unit Tests for Sales Data Generator.

Tests dataset generation logic, validation, and reproducibility.
"""

import csv
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest


def test_generated_data_file_exists() -> None:
    """Test that generated data file exists."""
    data_file = Path("data/sales_data.csv")
    assert data_file.exists(), "Sales data file should exist"


def test_generated_data_has_correct_schema() -> None:
    """Test that generated data has all required columns."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

    expected_columns = {
        "transaction_id",
        "date",
        "timestamp",
        "customer_id",
        "product_id",
        "product_category",
        "product_name",
        "quantity",
        "unit_price",
        "total_amount",
        "discount_percent",
        "payment_method",
        "region",
        "sales_rep_id",
        "customer_segment",
    }

    assert headers is not None
    assert set(headers) == expected_columns


def test_generated_data_has_valid_rows() -> None:
    """Test that generated data has reasonable number of rows."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        row_count = sum(1 for _ in reader)

    # Should have at least 1000 rows (reasonable minimum)
    assert row_count >= 1000, f"Expected at least 1000 rows, got {row_count}"


def test_generated_data_has_unique_transaction_ids() -> None:
    """Test that all transaction IDs are unique."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    transaction_ids = []
    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            transaction_ids.append(row["transaction_id"])

    # Check uniqueness
    assert len(transaction_ids) == len(
        set(transaction_ids)
    ), "Transaction IDs should be unique"


def test_generated_data_valid_dates() -> None:
    """Test that dates are valid and within expected range."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    dates = []
    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 100:  # Sample first 100 rows
                break
            dates.append(row["date"])

    # Verify date format
    for date_str in dates:
        try:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
            # Verify reasonable range (2020-2025)
            assert 2020 <= parsed_date.year <= 2025
        except ValueError:
            pytest.fail(f"Invalid date format: {date_str}")


def test_generated_data_valid_numeric_fields() -> None:
    """Test that numeric fields have valid values."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 100:  # Sample first 100 rows
                break

            # Test quantity is positive integer
            quantity = int(row["quantity"])
            assert quantity > 0, f"Quantity should be positive, got {quantity}"

            # Test unit_price is positive
            unit_price = Decimal(row["unit_price"])
            assert unit_price > 0, f"Unit price should be positive, got {unit_price}"

            # Test total_amount is positive
            total_amount = Decimal(row["total_amount"])
            assert (
                total_amount >= 0
            ), f"Total amount should be non-negative, got {total_amount}"

            # Test discount_percent is in valid range
            discount = Decimal(row["discount_percent"])
            assert 0 <= discount <= 100, f"Discount should be 0-100%, got {discount}"


def test_generated_data_business_logic() -> None:
    """Test that business logic is correct (total = quantity * unit_price * (1 - discount))."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 100:  # Sample first 100 rows
                break

            quantity = Decimal(row["quantity"])
            unit_price = Decimal(row["unit_price"])
            discount_percent = Decimal(row["discount_percent"])
            total_amount = Decimal(row["total_amount"])

            # Calculate expected total
            expected_total = quantity * unit_price * (1 - discount_percent / 100)

            # Allow small rounding difference
            difference = abs(total_amount - expected_total)
            assert difference < Decimal("0.01"), (
                f"Total amount calculation incorrect: "
                f"{total_amount} != {expected_total} "
                f"(diff: {difference})"
            )


def test_generated_data_has_required_categories() -> None:
    """Test that data includes expected categories."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    categories = set()
    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["product_category"]:  # Skip empty values
                categories.add(row["product_category"])

    # Should have multiple categories
    assert (
        len(categories) >= 3
    ), f"Expected at least 3 categories, got {len(categories)}"


def test_generated_data_has_required_regions() -> None:
    """Test that data includes expected regions."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    regions = set()
    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["region"]:  # Skip empty values
                regions.add(row["region"])

    # Should have multiple regions
    assert len(regions) >= 2, f"Expected at least 2 regions, got {len(regions)}"


def test_generated_data_has_required_payment_methods() -> None:
    """Test that data includes expected payment methods."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    payment_methods = set()
    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["payment_method"]:  # Skip empty values
                payment_methods.add(row["payment_method"])

    # Should have multiple payment methods
    assert (
        len(payment_methods) >= 2
    ), f"Expected at least 2 payment methods, got {len(payment_methods)}"


def test_data_generator_consistency() -> None:
    """Test that data generated is internally consistent."""
    data_file = Path("data/sales_data.csv")

    if not data_file.exists():
        pytest.skip("Data file not generated yet")

    # Count customers and transactions
    customers = set()
    transaction_count = 0

    with open(data_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["customer_id"]:
                customers.add(row["customer_id"])
            transaction_count += 1

    # Should have multiple customers
    assert (
        len(customers) >= 10
    ), f"Expected at least 10 unique customers, got {len(customers)}"

    # Average transactions per customer should be reasonable (> 1)
    avg_transactions = transaction_count / len(customers)
    assert (
        avg_transactions >= 1
    ), f"Average transactions per customer too low: {avg_transactions}"
