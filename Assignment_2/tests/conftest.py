"""
Pytest Configuration and Fixtures.

Provides common test fixtures for unit and integration tests.
"""

from decimal import Decimal

import pytest

from src.data_loader import SalesTransaction


@pytest.fixture
def sample_transactions() -> list[SalesTransaction]:
    """
    Fixture providing sample transactions for testing.

    Returns:
        List of sample SalesTransaction objects
    """
    return [
        SalesTransaction(
            transaction_id="TXN-0000001",
            date="2023-01-15",
            timestamp="2023-01-15 10:30:00",
            customer_id="CUST-00001",
            product_id="PROD-0001",
            product_category="Electronics",
            product_name="Wireless Headphones",
            quantity=2,
            unit_price=Decimal("99.99"),
            total_amount=Decimal("199.98"),
            discount_percent=0.0,
            payment_method="Credit Card",
            region="North",
            sales_rep_id="REP-001",
            customer_segment="Individual",
        ),
        SalesTransaction(
            transaction_id="TXN-0000002",
            date="2023-01-16",
            timestamp="2023-01-16 14:20:00",
            customer_id="CUST-00002",
            product_id="PROD-0002",
            product_category="Clothing",
            product_name="T-Shirt",
            quantity=5,
            unit_price=Decimal("19.99"),
            total_amount=Decimal("89.96"),
            discount_percent=10.0,
            payment_method="Debit Card",
            region="South",
            sales_rep_id="REP-002",
            customer_segment="SMB",
        ),
        SalesTransaction(
            transaction_id="TXN-0000003",
            date="2023-02-10",
            timestamp="2023-02-10 09:15:00",
            customer_id="CUST-00001",
            product_id="PROD-0003",
            product_category="Electronics",
            product_name="USB-C Cable",
            quantity=3,
            unit_price=Decimal("15.99"),
            total_amount=Decimal("47.97"),
            discount_percent=0.0,
            payment_method="Credit Card",
            region="North",
            sales_rep_id="REP-001",
            customer_segment="Individual",
        ),
        SalesTransaction(
            transaction_id="TXN-0000004",
            date="2023-03-05",
            timestamp="2023-03-05 16:45:00",
            customer_id="CUST-00003",
            product_id="PROD-0004",
            product_category="Books",
            product_name="Fiction Novel",
            quantity=1,
            unit_price=Decimal("25.00"),
            total_amount=Decimal("21.25"),
            discount_percent=15.0,
            payment_method="PayPal",
            region="East",
            sales_rep_id="REP-003",
            customer_segment="Enterprise",
        ),
        SalesTransaction(
            transaction_id="TXN-0000005",
            date="2023-04-20",
            timestamp="2023-04-20 11:00:00",
            customer_id="CUST-00002",
            product_id="PROD-0005",
            product_category="Sports & Outdoors",
            product_name="Yoga Mat",
            quantity=2,
            unit_price=Decimal("35.00"),
            total_amount=Decimal("70.00"),
            discount_percent=0.0,
            payment_method="Debit Card",
            region="South",
            sales_rep_id="REP-002",
            customer_segment="SMB",
        ),
    ]


@pytest.fixture
def empty_transactions() -> list[SalesTransaction]:
    """Fixture providing empty transaction list."""
    return []


@pytest.fixture
def single_transaction() -> list[SalesTransaction]:
    """Fixture providing single transaction."""
    return [
        SalesTransaction(
            transaction_id="TXN-0000001",
            date="2023-01-01",
            timestamp="2023-01-01 12:00:00",
            customer_id="CUST-00001",
            product_id="PROD-0001",
            product_category="Test",
            product_name="Test Product",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_amount=Decimal("100.00"),
            discount_percent=0.0,
            payment_method="Cash",
            region="Test Region",
            sales_rep_id="REP-001",
            customer_segment="Test Segment",
        )
    ]


@pytest.fixture
def transactions_with_nulls() -> list[SalesTransaction]:
    """Fixture providing transactions with null customer_id and sales_rep_id."""
    return [
        SalesTransaction(
            transaction_id="TXN-0000001",
            date="2023-01-01",
            timestamp="2023-01-01 10:00:00",
            customer_id=None,  # Null customer
            product_id="PROD-0001",
            product_category="Electronics",
            product_name="Gadget",
            quantity=1,
            unit_price=Decimal("50.00"),
            total_amount=Decimal("50.00"),
            discount_percent=0.0,
            payment_method="Cash",
            region="North",
            sales_rep_id=None,  # Null sales rep
            customer_segment="Individual",
        ),
        SalesTransaction(
            transaction_id="TXN-0000002",
            date="2023-01-02",
            timestamp="2023-01-02 11:00:00",
            customer_id="CUST-00001",
            product_id="PROD-0002",
            product_category="Books",
            product_name="Book",
            quantity=2,
            unit_price=Decimal("20.00"),
            total_amount=Decimal("40.00"),
            discount_percent=0.0,
            payment_method="Credit Card",
            region="South",
            sales_rep_id="REP-001",
            customer_segment="Individual",
        ),
    ]


@pytest.fixture
def large_transaction_set() -> list[SalesTransaction]:
    """Fixture providing larger set of transactions for performance testing."""
    transactions = []
    categories = ["Electronics", "Clothing", "Books", "Sports"]
    regions = ["North", "South", "East", "West"]
    segments = ["Individual", "SMB", "Enterprise"]

    for i in range(100):
        transactions.append(
            SalesTransaction(
                transaction_id=f"TXN-{i:07d}",
                date=f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                timestamp=f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d} 12:00:00",
                customer_id=f"CUST-{(i % 20):05d}",
                product_id=f"PROD-{(i % 50):04d}",
                product_category=categories[i % len(categories)],
                product_name=f"Product {i}",
                quantity=(i % 5) + 1,
                unit_price=Decimal(str((i % 100) + 10)),
                total_amount=Decimal(str(((i % 100) + 10) * ((i % 5) + 1))),
                discount_percent=float((i % 10) * 5),
                payment_method="Credit Card" if i % 2 == 0 else "Cash",
                region=regions[i % len(regions)],
                sales_rep_id=f"REP-{(i % 10):03d}",
                customer_segment=segments[i % len(segments)],
            )
        )

    return transactions
