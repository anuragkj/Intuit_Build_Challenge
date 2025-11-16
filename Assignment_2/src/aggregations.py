"""
Aggregation Operations Module.

Provides functional aggregation operations including:
- Sum, count, average
- Min, max
- Statistical operations

All operations use pure functional programming patterns:
- No mutation
- Composable with other operations
- Type-safe with generics
"""

from collections.abc import Callable, Iterable
from decimal import Decimal
from functools import reduce
from typing import Any, Optional, TypeVar, Union

T = TypeVar("T")
NumericType = Union[int, float, Decimal]


def sum_by(iterable: Iterable[T], key: Callable[[T], NumericType]) -> Decimal:
    """
    Sum numeric values extracted by key function.

    Uses functional reduce pattern for aggregation.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value from each element

    Returns:
        Sum as Decimal for precision

    Time Complexity: O(n)
    Space Complexity: O(1)

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 200}]
        >>> sum_by(transactions, lambda t: t['amount'])
        Decimal('300')
    """
    return Decimal(sum(Decimal(str(key(item))) for item in iterable))


def count_by(
    iterable: Iterable[T], predicate: Optional[Callable[[T], bool]] = None
) -> int:
    """
    Count elements, optionally filtered by predicate.

    Args:
        iterable: Source iterable
        predicate: Optional filter function

    Returns:
        Count of matching elements

    Time Complexity: O(n)
    Space Complexity: O(1)

    Example:
        >>> count_by([1, 2, 3, 4, 5], lambda x: x % 2 == 0)
        2
    """
    if predicate is None:
        return sum(1 for _ in iterable)
    return sum(1 for item in iterable if predicate(item))


def avg_by(iterable: Iterable[T], key: Callable[[T], NumericType]) -> Decimal:
    """
    Calculate average of numeric values extracted by key function.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value

    Returns:
        Average as Decimal

    Raises:
        ValueError: If iterable is empty

    Time Complexity: O(n)
    Space Complexity: O(n) - needs to materialize for two passes

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 200}]
        >>> avg_by(transactions, lambda t: t['amount'])
        Decimal('150')
    """
    items = list(iterable)  # Materialize to allow two passes
    if not items:
        raise ValueError("Cannot calculate average of empty sequence")

    total = sum_by(items, key)
    count = len(items)
    return total / Decimal(count)


def min_by(iterable: Iterable[T], key: Callable[[T], Any]) -> Optional[T]:
    """
    Find element with minimum value according to key function.

    Args:
        iterable: Source iterable
        key: Function to extract comparable value

    Returns:
        Element with minimum value, or None if empty

    Time Complexity: O(n)
    Space Complexity: O(1)

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 50}]
        >>> min_by(transactions, lambda t: t['amount'])
        {'amount': 50}
    """
    try:
        return min(iterable, key=key)
    except ValueError:
        return None


def max_by(iterable: Iterable[T], key: Callable[[T], Any]) -> Optional[T]:
    """
    Find element with maximum value according to key function.

    Args:
        iterable: Source iterable
        key: Function to extract comparable value

    Returns:
        Element with maximum value, or None if empty

    Time Complexity: O(n)
    Space Complexity: O(1)

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 50}]
        >>> max_by(transactions, lambda t: t['amount'])
        {'amount': 100}
    """
    try:
        return max(iterable, key=key)
    except ValueError:
        return None


def median_by(iterable: Iterable[T], key: Callable[[T], NumericType]) -> Decimal:
    """
    Calculate median of numeric values extracted by key function.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value

    Returns:
        Median value as Decimal

    Raises:
        ValueError: If iterable is empty

    Time Complexity: O(n log n) due to sorting
    Space Complexity: O(n)

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 200}, {'amount': 150}]
        >>> median_by(transactions, lambda t: t['amount'])
        Decimal('150')
    """
    values = sorted([Decimal(str(key(item))) for item in iterable])
    if not values:
        raise ValueError("Cannot calculate median of empty sequence")

    n = len(values)
    if n % 2 == 1:
        return values[n // 2]
    else:
        return (values[n // 2 - 1] + values[n // 2]) / Decimal(2)


def percentile_by(
    iterable: Iterable[T], key: Callable[[T], NumericType], percentile: float
) -> Decimal:
    """
    Calculate percentile of numeric values.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value
        percentile: Percentile to calculate (0-100)

    Returns:
        Value at given percentile

    Raises:
        ValueError: If iterable is empty or percentile out of range

    Time Complexity: O(n log n) due to sorting
    Space Complexity: O(n)

    Example:
        >>> data = [{'amount': i} for i in range(101)]
        >>> percentile_by(data, lambda t: t['amount'], 95)
        Decimal('95')
    """
    if not (0 <= percentile <= 100):
        raise ValueError("Percentile must be between 0 and 100")

    values = sorted([Decimal(str(key(item))) for item in iterable])
    if not values:
        raise ValueError("Cannot calculate percentile of empty sequence")

    if percentile == 100:
        return values[-1]

    index = (len(values) - 1) * (percentile / 100)
    lower = int(index)
    upper = lower + 1

    if upper >= len(values):
        return values[-1]

    weight = Decimal(str(index - lower))
    return values[lower] * (Decimal(1) - weight) + values[upper] * weight


def variance_by(iterable: Iterable[T], key: Callable[[T], NumericType]) -> Decimal:
    """
    Calculate variance of numeric values.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value

    Returns:
        Variance as Decimal

    Raises:
        ValueError: If iterable has fewer than 2 elements

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> data = [{'amount': 10}, {'amount': 20}, {'amount': 30}]
        >>> variance_by(data, lambda t: t['amount'])
        Decimal('66.666...')
    """
    items = list(iterable)
    if len(items) < 2:
        raise ValueError("Variance requires at least 2 elements")

    mean = avg_by(items, key)
    squared_diffs = sum((Decimal(str(key(item))) - mean) ** 2 for item in items)
    return squared_diffs / Decimal(len(items) - 1)


def std_dev_by(iterable: Iterable[T], key: Callable[[T], NumericType]) -> Decimal:
    """
    Calculate standard deviation of numeric values.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value

    Returns:
        Standard deviation as Decimal

    Raises:
        ValueError: If iterable has fewer than 2 elements

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> data = [{'amount': 10}, {'amount': 20}, {'amount': 30}]
        >>> std_dev_by(data, lambda t: t['amount'])
        Decimal('8.164...')
    """
    return variance_by(iterable, key).sqrt()


def product_by(iterable: Iterable[T], key: Callable[[T], NumericType]) -> Decimal:
    """
    Calculate product of numeric values extracted by key function.

    Uses functional reduce pattern.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value

    Returns:
        Product as Decimal

    Time Complexity: O(n)
    Space Complexity: O(1)

    Example:
        >>> data = [{'val': 2}, {'val': 3}, {'val': 4}]
        >>> product_by(data, lambda t: t['val'])
        Decimal('24')
    """
    return reduce(lambda acc, item: acc * Decimal(str(key(item))), iterable, Decimal(1))


def aggregate_multiple(
    iterable: Iterable[T],
    aggregations: dict[
        str, tuple[Callable[[T], NumericType], Callable[[Iterable[Decimal]], Decimal]]
    ],
) -> dict[str, Decimal]:
    """
    Perform multiple aggregations in a single pass.

    Efficient when you need multiple statistics on the same data.

    Args:
        iterable: Source iterable
        aggregations: Dict mapping names to (extractor, aggregator) tuples

    Returns:
        Dict of aggregation results

    Example:
        >>> data = [{'amount': 100}, {'amount': 200}]
        >>> aggregate_multiple(data, {
        ...     'total': (lambda t: t['amount'], sum),
        ...     'count': (lambda t: 1, sum)
        ... })
        {'total': Decimal('300'), 'count': Decimal('2')}
    """
    items = list(iterable)
    results = {}

    for name, (extractor, aggregator) in aggregations.items():
        values = [Decimal(str(extractor(item))) for item in items]
        results[name] = aggregator(values)

    return results
