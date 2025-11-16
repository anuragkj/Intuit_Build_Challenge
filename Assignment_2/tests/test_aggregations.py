"""
Unit Tests for Aggregation Operations Module.

Tests sum, average, min, max, and statistical operations.
"""

from decimal import Decimal

import pytest

from src.aggregations import (
    avg_by,
    count_by,
    max_by,
    median_by,
    min_by,
    percentile_by,
    product_by,
    std_dev_by,
    sum_by,
    variance_by,
)


def test_sum_by() -> None:
    """Test summing by key function."""
    data = [{"amount": 100}, {"amount": 200}, {"amount": 50}]
    result = sum_by(data, lambda d: d["amount"])
    assert result == Decimal("350")


def test_sum_by_empty() -> None:
    """Test sum of empty iterable."""
    result = sum_by([], lambda d: d["amount"])
    assert result == Decimal("0")


def test_count_by_no_predicate() -> None:
    """Test counting all elements."""
    assert count_by([1, 2, 3, 4, 5]) == 5
    assert count_by([]) == 0


def test_count_by_with_predicate() -> None:
    """Test counting with predicate."""
    is_even = lambda x: x % 2 == 0
    assert count_by([1, 2, 3, 4, 5, 6], is_even) == 3


def test_avg_by() -> None:
    """Test averaging by key function."""
    data = [{"amount": 100}, {"amount": 200}, {"amount": 50}]
    result = avg_by(data, lambda d: d["amount"])
    expected = Decimal("350") / Decimal("3")
    assert abs(result - expected) < Decimal("0.0000000000000000000000001")


def test_avg_by_empty() -> None:
    """Test average of empty iterable raises error."""
    with pytest.raises(ValueError, match="Cannot calculate average of empty sequence"):
        avg_by([], lambda d: d["amount"])


def test_min_by() -> None:
    """Test finding minimum by key function."""
    data = [{"amount": 100}, {"amount": 50}, {"amount": 200}]
    result = min_by(data, lambda d: d["amount"])
    assert result == {"amount": 50}


def test_min_by_empty() -> None:
    """Test minimum of empty iterable returns None."""
    result = min_by([], lambda d: d["amount"])
    assert result is None


def test_max_by() -> None:
    """Test finding maximum by key function."""
    data = [{"amount": 100}, {"amount": 50}, {"amount": 200}]
    result = max_by(data, lambda d: d["amount"])
    assert result == {"amount": 200}


def test_max_by_empty() -> None:
    """Test maximum of empty iterable returns None."""
    result = max_by([], lambda d: d["amount"])
    assert result is None


def test_median_by_odd_count() -> None:
    """Test median with odd number of elements."""
    data = [{"amount": 100}, {"amount": 200}, {"amount": 150}]
    result = median_by(data, lambda d: d["amount"])
    assert result == Decimal("150")


def test_median_by_even_count() -> None:
    """Test median with even number of elements."""
    data = [{"amount": 100}, {"amount": 200}, {"amount": 150}, {"amount": 250}]
    result = median_by(data, lambda d: d["amount"])
    assert result == Decimal("175")  # (150 + 200) / 2


def test_median_by_empty() -> None:
    """Test median of empty iterable raises error."""
    with pytest.raises(ValueError, match="Cannot calculate median of empty sequence"):
        median_by([], lambda d: d["amount"])


def test_percentile_by() -> None:
    """Test percentile calculation."""
    data = [{"amount": i} for i in range(101)]
    result = percentile_by(data, lambda d: d["amount"], 50.0)
    assert result == Decimal("50")

    result_95 = percentile_by(data, lambda d: d["amount"], 95.0)
    assert result_95 == Decimal("95")


def test_percentile_by_invalid_range() -> None:
    """Test percentile with invalid range raises error."""
    data = [{"amount": 100}]

    with pytest.raises(ValueError, match="Percentile must be between 0 and 100"):
        percentile_by(data, lambda d: d["amount"], 150.0)

    with pytest.raises(ValueError, match="Percentile must be between 0 and 100"):
        percentile_by(data, lambda d: d["amount"], -10.0)


def test_percentile_by_empty() -> None:
    """Test percentile of empty iterable raises error."""
    with pytest.raises(
        ValueError, match="Cannot calculate percentile of empty sequence"
    ):
        percentile_by([], lambda d: d["amount"], 50.0)


def test_variance_by() -> None:
    """Test variance calculation."""
    data = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
    result = variance_by(data, lambda d: d["amount"])
    # Sample variance of [10, 20, 30] is 100
    assert abs(float(result) - 100.0) < 0.01


def test_variance_by_insufficient_data() -> None:
    """Test variance with fewer than 2 elements raises error."""
    with pytest.raises(ValueError, match="Variance requires at least 2 elements"):
        variance_by([{"amount": 10}], lambda d: d["amount"])


def test_std_dev_by() -> None:
    """Test standard deviation calculation."""
    data = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
    result = std_dev_by(data, lambda d: d["amount"])
    # Sample std dev of [10, 20, 30] is 10
    assert abs(float(result) - 10.0) < 0.01


def test_product_by() -> None:
    """Test product calculation."""
    data = [{"val": 2}, {"val": 3}, {"val": 4}]
    result = product_by(data, lambda d: d["val"])
    assert result == Decimal("24")


def test_product_by_with_zero() -> None:
    """Test product with zero."""
    data = [{"val": 2}, {"val": 0}, {"val": 4}]
    result = product_by(data, lambda d: d["val"])
    assert result == Decimal("0")


def test_product_by_empty() -> None:
    """Test product of empty iterable returns 1."""
    result = product_by([], lambda d: d["val"])
    assert result == Decimal("1")
