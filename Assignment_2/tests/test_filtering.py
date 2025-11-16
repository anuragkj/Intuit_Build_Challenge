"""
Unit Tests for Filtering Operations Module.

Tests predicate-based filtering, range filtering, and filter composition.
"""

from decimal import Decimal

import pytest

from src.filtering import (
    any_filter,
    compose_filters,
    exclude_by,
    filter_bottom_n,
    filter_by,
    filter_date_range,
    filter_empty_strings,
    filter_none,
    filter_range,
    filter_top_n,
    filter_unique,
    not_filter,
)


def test_filter_by() -> None:
    """Test basic filtering with predicate."""
    data = [1, 2, 3, 4, 5, 6]
    is_even = lambda x: x % 2 == 0
    result = list(filter_by(data, is_even))
    assert result == [2, 4, 6]


def test_filter_by_empty() -> None:
    """Test filtering empty iterable."""
    result = list(filter_by([], lambda x: True))
    assert result == []


def test_exclude_by() -> None:
    """Test exclusion filtering (inverse of filter)."""
    data = [1, 2, 3, 4, 5, 6]
    is_even = lambda x: x % 2 == 0
    result = list(exclude_by(data, is_even))
    assert result == [1, 3, 5]


def test_exclude_by_all_match() -> None:
    """Test exclude when all elements match predicate."""
    data = [2, 4, 6]
    is_even = lambda x: x % 2 == 0
    result = list(exclude_by(data, is_even))
    assert result == []


def test_filter_range_inclusive() -> None:
    """Test range filtering with inclusive bounds."""
    transactions = [
        {"amount": 50},
        {"amount": 100},
        {"amount": 150},
        {"amount": 200},
        {"amount": 250},
    ]
    result = list(filter_range(transactions, lambda t: t["amount"], 100, 200))
    assert result == [{"amount": 100}, {"amount": 150}, {"amount": 200}]


def test_filter_range_exclusive() -> None:
    """Test range filtering with exclusive bounds."""
    transactions = [
        {"amount": 50},
        {"amount": 100},
        {"amount": 150},
        {"amount": 200},
    ]
    result = list(
        filter_range(transactions, lambda t: t["amount"], 100, 200, inclusive=False)
    )
    assert result == [{"amount": 150}]


def test_filter_range_with_decimal() -> None:
    """Test range filtering with Decimal values."""
    transactions = [
        {"amount": Decimal("50.50")},
        {"amount": Decimal("100.00")},
        {"amount": Decimal("150.75")},
    ]
    result = list(
        filter_range(transactions, lambda t: t["amount"], Decimal("75"), Decimal("175"))
    )
    assert len(result) == 2


def test_filter_date_range() -> None:
    """Test date range filtering."""
    transactions = [
        {"date": "2023-01-15"},
        {"date": "2023-06-20"},
        {"date": "2023-12-25"},
        {"date": "2024-01-10"},
    ]
    result = list(
        filter_date_range(transactions, lambda t: t["date"], "2023-01-01", "2023-12-31")
    )
    assert len(result) == 3
    assert result[0]["date"] == "2023-01-15"
    assert result[2]["date"] == "2023-12-25"


def test_filter_date_range_empty() -> None:
    """Test date range filtering with no matches."""
    transactions = [{"date": "2023-01-15"}, {"date": "2023-06-20"}]
    result = list(
        filter_date_range(transactions, lambda t: t["date"], "2024-01-01", "2024-12-31")
    )
    assert result == []


def test_filter_top_n() -> None:
    """Test getting top N elements."""
    transactions = [
        {"amount": 100},
        {"amount": 500},
        {"amount": 200},
        {"amount": 300},
        {"amount": 150},
    ]
    result = filter_top_n(transactions, lambda t: t["amount"], 3)
    amounts = [t["amount"] for t in result]
    assert amounts == [500, 300, 200]


def test_filter_top_n_more_than_available() -> None:
    """Test top N when N > list size."""
    transactions = [{"amount": 100}, {"amount": 200}]
    result = filter_top_n(transactions, lambda t: t["amount"], 10)
    assert len(result) == 2


def test_filter_bottom_n() -> None:
    """Test getting bottom N elements."""
    transactions = [
        {"amount": 100},
        {"amount": 500},
        {"amount": 200},
        {"amount": 300},
        {"amount": 150},
    ]
    result = filter_bottom_n(transactions, lambda t: t["amount"], 3)
    amounts = [t["amount"] for t in result]
    assert amounts == [100, 150, 200]


def test_filter_none() -> None:
    """Test filtering out None values."""
    data = [1, None, 2, None, 3, None, 4]
    result = list(filter_none(data))
    assert result == [1, 2, 3, 4]


def test_filter_none_all_none() -> None:
    """Test filter_none when all values are None."""
    data = [None, None, None]
    result = list(filter_none(data))
    assert result == []


def test_filter_empty_strings() -> None:
    """Test filtering out empty strings."""
    data = ["hello", "", "world", "  ", "test"]
    result = list(filter_empty_strings(data))
    assert result == ["hello", "world", "test"]


def test_filter_unique_without_key() -> None:
    """Test unique filtering without key function."""
    data = [1, 2, 2, 3, 1, 4, 3, 5]
    result = list(filter_unique(data))
    assert result == [1, 2, 3, 4, 5]


def test_filter_unique_with_key() -> None:
    """Test unique filtering with key function."""
    data = [
        {"id": 1, "val": "a"},
        {"id": 2, "val": "b"},
        {"id": 1, "val": "c"},
        {"id": 3, "val": "d"},
    ]
    result = list(filter_unique(data, lambda x: x["id"]))
    assert len(result) == 3
    assert result[0]["id"] == 1
    assert result[0]["val"] == "a"  # First occurrence preserved


def test_compose_filters() -> None:
    """Test composing multiple filters with AND logic."""
    is_positive = lambda x: x > 0
    is_even = lambda x: x % 2 == 0
    is_less_than_10 = lambda x: x < 10

    combined = compose_filters(is_positive, is_even, is_less_than_10)
    result = list(filter(combined, [-2, -1, 0, 1, 2, 3, 4, 5, 6, 10, 12]))
    assert result == [2, 4, 6]


def test_compose_filters_single_predicate() -> None:
    """Test composing single predicate."""
    is_even = lambda x: x % 2 == 0
    combined = compose_filters(is_even)
    result = list(filter(combined, [1, 2, 3, 4]))
    assert result == [2, 4]


def test_any_filter() -> None:
    """Test composing filters with OR logic."""
    is_negative = lambda x: x < 0
    is_even = lambda x: x % 2 == 0

    combined = any_filter(is_negative, is_even)
    result = list(filter(combined, [-2, -1, 0, 1, 2, 3, 4]))
    assert result == [-2, -1, 0, 2, 4]


def test_any_filter_none_match() -> None:
    """Test any_filter when no predicates match."""
    is_negative = lambda x: x < 0
    is_greater_than_10 = lambda x: x > 10

    combined = any_filter(is_negative, is_greater_than_10)
    result = list(filter(combined, [1, 2, 3, 4, 5]))
    assert result == []


def test_not_filter() -> None:
    """Test negating a predicate."""
    is_even = lambda x: x % 2 == 0
    is_odd = not_filter(is_even)
    result = list(filter(is_odd, [1, 2, 3, 4, 5, 6]))
    assert result == [1, 3, 5]


def test_complex_filter_composition() -> None:
    """Test complex filter composition."""
    # Find numbers that are either negative OR (positive AND even)
    is_negative = lambda x: x < 0
    is_positive = lambda x: x > 0
    is_even = lambda x: x % 2 == 0

    positive_and_even = compose_filters(is_positive, is_even)
    combined = any_filter(is_negative, positive_and_even)

    result = list(filter(combined, [-3, -2, -1, 0, 1, 2, 3, 4, 5, 6]))
    assert result == [-3, -2, -1, 2, 4, 6]
