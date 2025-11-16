"""
Filtering Operations Module.

Provides functional filtering operations:
- Predicate-based filtering
- Range filtering
- Composition of filters
- Exclusion filters

Demonstrates functional filter patterns and lambda expressions.
"""

from collections.abc import Callable, Iterable
from datetime import datetime
from decimal import Decimal
from typing import Any, TypeVar, Union

T = TypeVar("T")
NumericType = Union[int, float, Decimal]


def filter_by(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Iterable[T]:
    """
    Filter elements using predicate function.

    Wrapper around built-in filter for consistency.
    Returns iterator for lazy evaluation.

    Args:
        iterable: Source iterable
        predicate: Boolean function to test elements

    Returns:
        Iterator of matching elements

    Time Complexity: O(1) to create, O(n) to consume
    Space Complexity: O(1)

    Example:
        >>> data = [1, 2, 3, 4, 5]
        >>> list(filter_by(data, lambda x: x % 2 == 0))
        [2, 4]
    """
    return filter(predicate, iterable)


def exclude_by(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Iterable[T]:
    """
    Exclude elements matching predicate (inverse of filter).

    Args:
        iterable: Source iterable
        predicate: Boolean function to test elements

    Returns:
        Iterator of non-matching elements

    Time Complexity: O(1) to create, O(n) to consume
    Space Complexity: O(1)

    Example:
        >>> data = [1, 2, 3, 4, 5]
        >>> list(exclude_by(data, lambda x: x % 2 == 0))
        [1, 3, 5]
    """
    from itertools import filterfalse

    return filterfalse(predicate, iterable)


def filter_range(
    iterable: Iterable[T],
    key: Callable[[T], NumericType],
    min_val: NumericType,
    max_val: NumericType,
    inclusive: bool = True,
) -> Iterable[T]:
    """
    Filter elements within numeric range.

    Args:
        iterable: Source iterable
        key: Function to extract numeric value
        min_val: Minimum value (inclusive or exclusive)
        max_val: Maximum value (inclusive or exclusive)
        inclusive: Whether bounds are inclusive

    Returns:
        Iterator of elements within range

    Time Complexity: O(1) to create, O(n) to consume
    Space Complexity: O(1)

    Example:
        >>> transactions = [{'amount': 50}, {'amount': 150}, {'amount': 250}]
        >>> list(filter_range(transactions, lambda t: t['amount'], 100, 200))
        [{'amount': 150}]
    """
    if inclusive:
        predicate = lambda item: min_val <= key(item) <= max_val
    else:
        predicate = lambda item: min_val < key(item) < max_val

    return filter_by(iterable, predicate)


def filter_date_range(
    iterable: Iterable[T],
    key: Callable[[T], str],
    start_date: str,
    end_date: str,
    date_format: str = "%Y-%m-%d",
) -> Iterable[T]:
    """
    Filter elements within date range.

    Args:
        iterable: Source iterable
        key: Function to extract date string
        start_date: Start date string
        end_date: End date string
        date_format: Date format string

    Returns:
        Iterator of elements within date range

    Time Complexity: O(1) to create, O(n) to consume
    Space Complexity: O(1)

    Example:
        >>> transactions = [
        ...     {'date': '2023-01-15'},
        ...     {'date': '2023-06-20'},
        ...     {'date': '2024-01-10'}
        ... ]
        >>> list(filter_date_range(
        ...     transactions,
        ...     lambda t: t['date'],
        ...     '2023-01-01',
        ...     '2023-12-31'
        ... ))
        [{'date': '2023-01-15'}, {'date': '2023-06-20'}]
    """
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)

    def predicate(item: T) -> bool:
        date_str = key(item)
        date = datetime.strptime(date_str, date_format)
        return start <= date <= end

    return filter_by(iterable, predicate)


def filter_top_n(iterable: Iterable[T], key: Callable[[T], Any], n: int) -> list[T]:
    """
    Get top N elements by key function.

    More efficient than sorting entire sequence.

    Args:
        iterable: Source iterable
        key: Function to extract comparable value
        n: Number of top elements to return

    Returns:
        List of top N elements

    Time Complexity: O(n log k) using heap
    Space Complexity: O(k) where k is n

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 500}, {'amount': 200}]
        >>> filter_top_n(transactions, lambda t: t['amount'], 2)
        [{'amount': 500}, {'amount': 200}]
    """
    from heapq import nlargest

    return nlargest(n, iterable, key=key)


def filter_bottom_n(iterable: Iterable[T], key: Callable[[T], Any], n: int) -> list[T]:
    """
    Get bottom N elements by key function.

    Args:
        iterable: Source iterable
        key: Function to extract comparable value
        n: Number of bottom elements to return

    Returns:
        List of bottom N elements

    Time Complexity: O(n log k) using heap
    Space Complexity: O(k) where k is n

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 500}, {'amount': 200}]
        >>> filter_bottom_n(transactions, lambda t: t['amount'], 2)
        [{'amount': 100}, {'amount': 200}]
    """
    from heapq import nsmallest

    return nsmallest(n, iterable, key=key)


def filter_none(iterable: Iterable[T | None]) -> Iterable[T]:
    """
    Filter out None values.

    Args:
        iterable: Source iterable potentially containing None

    Returns:
        Iterator with None values removed

    Example:
        >>> list(filter_none([1, None, 2, None, 3]))
        [1, 2, 3]
    """
    return (item for item in iterable if item is not None)


def filter_empty_strings(iterable: Iterable[str]) -> Iterable[str]:
    """
    Filter out empty strings.

    Args:
        iterable: Source iterable of strings

    Returns:
        Iterator with empty strings removed

    Example:
        >>> list(filter_empty_strings(['a', '', 'b', '', 'c']))
        ['a', 'b', 'c']
    """
    return (item for item in iterable if item.strip())


def filter_unique(
    iterable: Iterable[T], key: Callable[[T], Any] | None = None
) -> Iterable[T]:
    """
    Filter to unique elements based on key function.

    Preserves order of first occurrence.

    Args:
        iterable: Source iterable
        key: Optional function to extract comparison value

    Returns:
        Iterator of unique elements

    Time Complexity: O(n)
    Space Complexity: O(k) where k is number of unique keys

    Example:
        >>> data = [{'id': 1, 'val': 'a'}, {'id': 2, 'val': 'b'}, {'id': 1, 'val': 'c'}]
        >>> list(filter_unique(data, lambda x: x['id']))
        [{'id': 1, 'val': 'a'}, {'id': 2, 'val': 'b'}]
    """
    seen: set[Any] = set()
    for item in iterable:
        k = key(item) if key else item
        if k not in seen:
            seen.add(k)
            yield item


def compose_filters(*predicates: Callable[[T], bool]) -> Callable[[T], bool]:
    """
    Compose multiple predicates with AND logic.

    Args:
        *predicates: Variable number of predicate functions

    Returns:
        Combined predicate (all must be true)

    Example:
        >>> is_positive = lambda x: x > 0
        >>> is_even = lambda x: x % 2 == 0
        >>> is_positive_even = compose_filters(is_positive, is_even)
        >>> list(filter(is_positive_even, [-2, -1, 0, 1, 2, 3, 4]))
        [2, 4]
    """

    def combined(item: T) -> bool:
        return all(predicate(item) for predicate in predicates)

    return combined


def any_filter(*predicates: Callable[[T], bool]) -> Callable[[T], bool]:
    """
    Compose multiple predicates with OR logic.

    Args:
        *predicates: Variable number of predicate functions

    Returns:
        Combined predicate (any can be true)

    Example:
        >>> is_negative = lambda x: x < 0
        >>> is_even = lambda x: x % 2 == 0
        >>> is_negative_or_even = any_filter(is_negative, is_even)
        >>> list(filter(is_negative_or_even, [-2, -1, 0, 1, 2, 3, 4]))
        [-2, -1, 0, 2, 4]
    """

    def combined(item: T) -> bool:
        return any(predicate(item) for predicate in predicates)

    return combined


def not_filter(predicate: Callable[[T], bool]) -> Callable[[T], bool]:
    """
    Negate a predicate.

    Args:
        predicate: Predicate function to negate

    Returns:
        Negated predicate

    Example:
        >>> is_even = lambda x: x % 2 == 0
        >>> is_odd = not_filter(is_even)
        >>> list(filter(is_odd, [1, 2, 3, 4, 5]))
        [1, 3, 5]
    """

    def negated(item: T) -> bool:
        return not predicate(item)

    return negated
