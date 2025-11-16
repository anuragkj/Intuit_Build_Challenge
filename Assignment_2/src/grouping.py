"""
Grouping Operations Module.

Provides functional grouping operations for data aggregation:
- Single-level grouping (group_by)
- Multi-level grouping (nested grouping)
- Partitioning

Implements patterns equivalent to SQL GROUP BY and functional groupBy.
"""

from collections import defaultdict
from collections.abc import Callable, Iterable
from itertools import groupby as itertools_groupby
from typing import Any, TypeVar

T = TypeVar("T")
K = TypeVar("K")


def group_by(iterable: Iterable[T], key_func: Callable[[T], K]) -> dict[K, list[T]]:
    """
    Group elements by key function.

    Demonstrates functional grouping pattern, equivalent to:
    - SQL: GROUP BY
    - Java Streams: groupingBy()
    - Python itertools.groupby (but more convenient)

    Args:
        iterable: Source iterable
        key_func: Function to extract grouping key from each element

    Returns:
        Dictionary mapping keys to lists of elements

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> transactions = [
        ...     {'category': 'A', 'amount': 100},
        ...     {'category': 'B', 'amount': 200},
        ...     {'category': 'A', 'amount': 150}
        ... ]
        >>> group_by(transactions, lambda t: t['category'])
        {
            'A': [{'category': 'A', 'amount': 100}, {'category': 'A', 'amount': 150}],
            'B': [{'category': 'B', 'amount': 200}]
        }
    """
    groups: dict[K, list[T]] = defaultdict(list)
    for item in iterable:
        key = key_func(item)
        groups[key].append(item)
    return dict(groups)


def group_by_sorted(
    iterable: Iterable[T], key_func: Callable[[T], K]
) -> dict[K, list[T]]:
    """
    Group elements by key function using itertools.groupby.

    More memory-efficient than group_by() if data is already sorted,
    but requires sorting otherwise. Demonstrates use of itertools.groupby.

    Args:
        iterable: Source iterable (should be sorted by key_func)
        key_func: Function to extract grouping key

    Returns:
        Dictionary mapping keys to lists of elements

    Time Complexity: O(n log n) if sorting needed, O(n) if pre-sorted
    Space Complexity: O(n)

    Note:
        itertools.groupby requires consecutive equal keys.
        This function handles sorting automatically.

    Example:
        >>> data = [{'cat': 'B', 'val': 2}, {'cat': 'A', 'val': 1}, {'cat': 'A', 'val': 3}]
        >>> group_by_sorted(data, lambda t: t['cat'])
        {'A': [{'cat': 'A', 'val': 1}, {'cat': 'A', 'val': 3}], 'B': [{'cat': 'B', 'val': 2}]}
    """
    # Sort first to enable itertools.groupby
    sorted_items = sorted(iterable, key=key_func)  # type: ignore[arg-type]

    groups: dict[K, list[T]] = {}
    for key, group_iter in itertools_groupby(sorted_items, key=key_func):
        groups[key] = list(group_iter)

    return groups


def group_by_multiple(
    iterable: Iterable[T], *key_funcs: Callable[[T], Any]
) -> dict[tuple[Any, ...], list[T]]:
    """
    Group by multiple keys (composite key).

    Creates groups based on combination of multiple key functions.

    Args:
        iterable: Source iterable
        *key_funcs: Variable number of key extraction functions

    Returns:
        Dictionary mapping tuples of keys to lists of elements

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> transactions = [
        ...     {'region': 'North', 'category': 'A', 'amount': 100},
        ...     {'region': 'North', 'category': 'B', 'amount': 200},
        ...     {'region': 'South', 'category': 'A', 'amount': 150}
        ... ]
        >>> group_by_multiple(
        ...     transactions,
        ...     lambda t: t['region'],
        ...     lambda t: t['category']
        ... )
        {
            ('North', 'A'): [{'region': 'North', 'category': 'A', 'amount': 100}],
            ('North', 'B'): [{'region': 'North', 'category': 'B', 'amount': 200}],
            ('South', 'A'): [{'region': 'South', 'category': 'A', 'amount': 150}]
        }
    """
    groups: dict[tuple[Any, ...], list[T]] = defaultdict(list)
    for item in iterable:
        composite_key = tuple(key_func(item) for key_func in key_funcs)
        groups[composite_key].append(item)
    return dict(groups)


def nested_group_by(
    iterable: Iterable[T], *key_funcs: Callable[[T], Any]
) -> Any:
    """
    Create nested grouping structure.

    Groups data hierarchically by applying key functions in sequence.

    Args:
        iterable: Source iterable
        *key_funcs: Key functions to apply at each nesting level

    Returns:
        Nested dictionary structure, or list if no key functions provided

    Time Complexity: O(n * k) where k is number of key functions
    Space Complexity: O(n)

    Example:
        >>> transactions = [
        ...     {'region': 'North', 'category': 'A', 'amount': 100},
        ...     {'region': 'North', 'category': 'A', 'amount': 150},
        ...     {'region': 'South', 'category': 'B', 'amount': 200}
        ... ]
        >>> nested_group_by(
        ...     transactions,
        ...     lambda t: t['region'],
        ...     lambda t: t['category']
        ... )
        {
            'North': {
                'A': [{'region': 'North', 'category': 'A', 'amount': 100},
                      {'region': 'North', 'category': 'A', 'amount': 150}]
            },
            'South': {
                'B': [{'region': 'South', 'category': 'B', 'amount': 200}]
            }
        }
    """
    if not key_funcs:
        return list(iterable)

    first_key = key_funcs[0]
    remaining_keys = key_funcs[1:]

    groups = group_by(iterable, first_key)

    if not remaining_keys:
        return groups

    # Recursively group at deeper levels
    return {
        key: nested_group_by(items, *remaining_keys) for key, items in groups.items()
    }


def partition_by(
    iterable: Iterable[T], predicate: Callable[[T], bool]
) -> tuple[list[T], list[T]]:
    """
    Partition elements into two groups based on predicate.

    Separates elements into those matching and not matching predicate.
    Equivalent to filter and filterfalse but returns both in one pass.

    Args:
        iterable: Source iterable
        predicate: Boolean function to test elements

    Returns:
        Tuple of (matching elements, non-matching elements)

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> transactions = [
        ...     {'amount': 100, 'has_discount': True},
        ...     {'amount': 200, 'has_discount': False},
        ...     {'amount': 150, 'has_discount': True}
        ... ]
        >>> with_discount, without_discount = partition_by(
        ...     transactions,
        ...     lambda t: t['has_discount']
        ... )
    """
    matching: list[T] = []
    non_matching: list[T] = []

    for item in iterable:
        if predicate(item):
            matching.append(item)
        else:
            non_matching.append(item)

    return matching, non_matching


def index_by(iterable: Iterable[T], key_func: Callable[[T], K]) -> dict[K, T]:
    """
    Create dictionary indexed by key function.

    Similar to group_by but assumes keys are unique (or keeps last occurrence).
    Useful for creating lookup tables.

    Args:
        iterable: Source iterable
        key_func: Function to extract unique key

    Returns:
        Dictionary mapping keys to elements

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> users = [
        ...     {'id': 1, 'name': 'Alice'},
        ...     {'id': 2, 'name': 'Bob'},
        ...     {'id': 3, 'name': 'Charlie'}
        ... ]
        >>> index_by(users, lambda u: u['id'])
        {1: {'id': 1, 'name': 'Alice'}, 2: {'id': 2, 'name': 'Bob'}, ...}
    """
    return {key_func(item): item for item in iterable}


def group_and_aggregate(
    iterable: Iterable[T],
    key_func: Callable[[T], K],
    aggregate_func: Callable[[list[T]], Any],
) -> dict[K, Any]:
    """
    Group by key and apply aggregation function to each group.

    Combines grouping and aggregation in single operation.
    Demonstrates higher-order functional pattern.

    Args:
        iterable: Source iterable
        key_func: Function to extract grouping key
        aggregate_func: Function to aggregate each group

    Returns:
        Dictionary mapping keys to aggregated values

    Time Complexity: O(n + g*a) where g is groups, a is aggregation cost
    Space Complexity: O(n)

    Example:
        >>> transactions = [
        ...     {'category': 'A', 'amount': 100},
        ...     {'category': 'A', 'amount': 150},
        ...     {'category': 'B', 'amount': 200}
        ... ]
        >>> group_and_aggregate(
        ...     transactions,
        ...     lambda t: t['category'],
        ...     lambda group: sum(t['amount'] for t in group)
        ... )
        {'A': 250, 'B': 200}
    """
    groups = group_by(iterable, key_func)
    return {key: aggregate_func(items) for key, items in groups.items()}


def count_by_key(iterable: Iterable[T], key_func: Callable[[T], K]) -> dict[K, int]:
    """
    Count occurrences of each key.

    Efficient way to get frequency distribution.

    Args:
        iterable: Source iterable
        key_func: Function to extract key

    Returns:
        Dictionary mapping keys to counts

    Time Complexity: O(n)
    Space Complexity: O(k) where k is number of unique keys

    Example:
        >>> transactions = [
        ...     {'category': 'A'}, {'category': 'B'},
        ...     {'category': 'A'}, {'category': 'A'}
        ... ]
        >>> count_by_key(transactions, lambda t: t['category'])
        {'A': 3, 'B': 1}
    """
    counts: dict[K, int] = defaultdict(int)
    for item in iterable:
        key = key_func(item)
        counts[key] += 1
    return dict(counts)
