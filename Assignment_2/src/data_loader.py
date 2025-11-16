"""
Data Loader Module.

Provides CSV parsing functionality using functional programming principles.
Uses namedtuple for immutable data records and csv.DictReader for lazy evaluation.

Key Features:
    - Lazy evaluation: Processes data as iterator, not loading all into memory
    - Immutability: namedtuple ensures data cannot be modified
    - Type safety: Strong typing with Decimal for monetary values
    - Validation: Handles missing and malformed data gracefully
"""

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class SalesTransaction:
    """
    Immutable data class representing a single sales transaction.

    Attributes:
        transaction_id: Unique transaction identifier
        date: Transaction date (YYYY-MM-DD)
        timestamp: Full timestamp (YYYY-MM-DD HH:MM:SS)
        customer_id: Customer identifier (may be None)
        product_id: Product SKU
        product_category: Product category
        product_name: Product name
        quantity: Units sold
        unit_price: Price per unit
        total_amount: Total transaction amount
        discount_percent: Discount percentage applied
        payment_method: Payment method used
        region: Geographic region
        sales_rep_id: Sales representative ID (may be None)
        customer_segment: Customer segment classification
    """

    transaction_id: str
    date: str
    timestamp: str
    customer_id: Optional[str]
    product_id: str
    product_category: str
    product_name: str
    quantity: int
    unit_price: Decimal
    total_amount: Decimal
    discount_percent: float
    payment_method: str
    region: str
    sales_rep_id: Optional[str]
    customer_segment: str


def parse_decimal(value: str) -> Decimal:
    """
    Parse string to Decimal with error handling.

    Args:
        value: String representation of decimal number

    Returns:
        Decimal value

    Raises:
        ValueError: If value cannot be parsed as decimal
    """
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Cannot parse '{value}' as decimal") from e


def parse_int(value: str) -> int:
    """
    Parse string to integer with error handling.

    Args:
        value: String representation of integer

    Returns:
        Integer value

    Raises:
        ValueError: If value cannot be parsed as integer
    """
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(f"Cannot parse '{value}' as integer") from e


def parse_float(value: str) -> float:
    """
    Parse string to float with error handling.

    Args:
        value: String representation of float

    Returns:
        Float value

    Raises:
        ValueError: If value cannot be parsed as float
    """
    try:
        return float(value)
    except ValueError as e:
        raise ValueError(f"Cannot parse '{value}' as float") from e


def parse_optional_string(value: str) -> Optional[str]:
    """
    Parse string, returning None for empty strings.

    Args:
        value: String value

    Returns:
        String or None if empty
    """
    return value.strip() if value.strip() else None


def parse_row(row: dict[str, str]) -> SalesTransaction:
    """
    Parse a CSV row dictionary into a SalesTransaction.

    Args:
        row: Dictionary from csv.DictReader

    Returns:
        SalesTransaction instance

    Raises:
        ValueError: If row data is invalid or missing required fields
        KeyError: If required column is missing
    """
    try:
        return SalesTransaction(
            transaction_id=row["transaction_id"],
            date=row["date"],
            timestamp=row["timestamp"],
            customer_id=parse_optional_string(row.get("customer_id", "")),
            product_id=row["product_id"],
            product_category=row["product_category"],
            product_name=row["product_name"],
            quantity=parse_int(row["quantity"]),
            unit_price=parse_decimal(row["unit_price"]),
            total_amount=parse_decimal(row["total_amount"]),
            discount_percent=parse_float(row["discount_percent"]),
            payment_method=row["payment_method"],
            region=row["region"],
            sales_rep_id=parse_optional_string(row.get("sales_rep_id", "")),
            customer_segment=row["customer_segment"],
        )
    except KeyError as e:
        raise ValueError(f"Missing required column: {e}") from e


def load_csv(file_path: str) -> Iterator[SalesTransaction]:
    """
    Load sales data from CSV file as lazy iterator.

    This function demonstrates functional programming principles:
    - Lazy evaluation: Returns iterator, not list
    - Memory efficient: Processes one row at a time
    - Composable: Can be chained with other functional operations

    Args:
        file_path: Path to CSV file

    Yields:
        SalesTransaction instances

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV has invalid data

    Time Complexity: O(1) per row yielded
    Space Complexity: O(1) - constant memory usage

    Example:
        >>> transactions = load_csv('data/sales_data.csv')
        >>> first_ten = list(islice(transactions, 10))
    """
    csv_file = Path(file_path)

    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    with csv_file.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
            try:
                yield parse_row(row)
            except (ValueError, KeyError) as e:
                # Log error but continue processing (robustness)
                # In production, might want to collect errors or fail fast
                print(f"Warning: Skipping row {row_num}: {e}")
                continue


def load_csv_as_list(file_path: str) -> list[SalesTransaction]:
    """
    Load all transactions into memory as a list.

    Use this when you need random access or multiple passes over data.
    For large datasets, prefer load_csv() for memory efficiency.

    Args:
        file_path: Path to CSV file

    Returns:
        List of SalesTransaction instances

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV has invalid data

    Time Complexity: O(n) where n is number of rows
    Space Complexity: O(n)
    """
    return list(load_csv(file_path))


def validate_transaction(transaction: SalesTransaction) -> bool:
    """
    Validate transaction business logic.

    Checks:
    - Quantity is positive
    - Prices are non-negative
    - Discount is within valid range

    Args:
        transaction: Transaction to validate

    Returns:
        True if valid, False otherwise

    Example:
        >>> valid_transactions = filter(validate_transaction, transactions)
    """
    if transaction.quantity <= 0:
        return False
    if transaction.unit_price < 0:
        return False
    if transaction.total_amount < 0:
        return False
    if not (0 <= transaction.discount_percent <= 100):
        return False
    return True
