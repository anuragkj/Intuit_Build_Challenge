"""
Transformation Operations Module.

Provides functional transformation operations:
- Mapping (extract, transform)
- FlatMap (flatten nested structures)
- Projection (select fields)
- Enrichment (add computed fields)

Demonstrates map operations and functional transformations.
"""

from collections.abc import Callable, Iterable
from typing import Any, TypeVar

T = TypeVar("T")
U = TypeVar("U")


def map_by(iterable: Iterable[T], func: Callable[[T], U]) -> Iterable[U]:
    """
    Transform elements using mapping function.

    Wrapper around built-in map for consistency with other operations.

    Args:
        iterable: Source iterable
        func: Transformation function

    Returns:
        Iterator of transformed elements

    Time Complexity: O(1) to create, O(n) to consume
    Space Complexity: O(1)

    Example:
        >>> data = [1, 2, 3]
        >>> list(map_by(data, lambda x: x * 2))
        [2, 4, 6]
    """
    return map(func, iterable)


def extract_field(iterable: Iterable[T], key: Callable[[T], U]) -> Iterable[U]:
    """
    Extract single field from each element.

    Args:
        iterable: Source iterable
        key: Function to extract value

    Returns:
        Iterator of extracted values

    Example:
        >>> transactions = [{'amount': 100}, {'amount': 200}]
        >>> list(extract_field(transactions, lambda t: t['amount']))
        [100, 200]
    """
    return map(key, iterable)


def extract_fields(
    iterable: Iterable[T], *keys: Callable[[T], Any]
) -> Iterable[tuple[Any, ...]]:
    """
    Extract multiple fields from each element.

    Args:
        iterable: Source iterable
        *keys: Variable number of extraction functions

    Returns:
        Iterator of tuples containing extracted values

    Example:
        >>> transactions = [
        ...     {'id': 1, 'amount': 100, 'category': 'A'},
        ...     {'id': 2, 'amount': 200, 'category': 'B'}
        ... ]
        >>> list(extract_fields(
        ...     transactions,
        ...     lambda t: t['id'],
        ...     lambda t: t['amount']
        ... ))
        [(1, 100), (2, 200)]
    """
    return (tuple(key(item) for key in keys) for item in iterable)


def project(
    iterable: Iterable[dict[str, Any]], *field_names: str
) -> Iterable[dict[str, Any]]:
    """
    Project (select) specific fields from dictionaries.

    Similar to SQL SELECT with specific columns.

    Args:
        iterable: Source iterable of dictionaries
        *field_names: Field names to include in projection

    Returns:
        Iterator of dictionaries with only specified fields

    Example:
        >>> transactions = [
        ...     {'id': 1, 'amount': 100, 'category': 'A', 'date': '2023-01-01'},
        ...     {'id': 2, 'amount': 200, 'category': 'B', 'date': '2023-01-02'}
        ... ]
        >>> list(project(transactions, 'id', 'amount'))
        [{'id': 1, 'amount': 100}, {'id': 2, 'amount': 200}]
    """
    return (
        {field: item[field] for field in field_names if field in item}
        for item in iterable
    )


def add_computed_field(
    iterable: Iterable[dict[str, Any]],
    field_name: str,
    compute_func: Callable[[dict[str, Any]], Any],
) -> Iterable[dict[str, Any]]:
    """
    Add computed field to each dictionary.

    Creates new dictionaries with additional computed field.
    Demonstrates immutability (returns new dicts, doesn't mutate).

    Args:
        iterable: Source iterable of dictionaries
        field_name: Name of new field
        compute_func: Function to compute field value

    Returns:
        Iterator of enriched dictionaries

    Example:
        >>> transactions = [{'price': 100, 'quantity': 2}, {'price': 50, 'quantity': 3}]
        >>> list(add_computed_field(
        ...     transactions,
        ...     'total',
        ...     lambda t: t['price'] * t['quantity']
        ... ))
        [{'price': 100, 'quantity': 2, 'total': 200},
         {'price': 50, 'quantity': 3, 'total': 150}]
    """
    for item in iterable:
        new_item = item.copy()
        new_item[field_name] = compute_func(item)
        yield new_item


def flatmap(iterable: Iterable[T], func: Callable[[T], Iterable[U]]) -> Iterable[U]:
    """
    Map and flatten (flatMap operation).

    Applies function that returns iterable, then flattens result.

    Args:
        iterable: Source iterable
        func: Function that returns iterable

    Returns:
        Flattened iterator

    Time Complexity: O(1) to create, O(n*m) to consume
    Space Complexity: O(1)

    Example:
        >>> data = [[1, 2], [3, 4], [5]]
        >>> list(flatmap(data, lambda x: x))
        [1, 2, 3, 4, 5]

        >>> words = ['hello', 'world']
        >>> list(flatmap(words, lambda w: list(w)))
        ['h', 'e', 'l', 'l', 'o', 'w', 'o', 'r', 'l', 'd']
    """
    from itertools import chain

    return chain.from_iterable(map(func, iterable))


def enumerate_with(iterable: Iterable[T], start: int = 0) -> Iterable[tuple[int, T]]:
    """
    Enumerate elements with index.

    Wrapper around built-in enumerate for consistency.

    Args:
        iterable: Source iterable
        start: Starting index

    Returns:
        Iterator of (index, element) tuples

    Example:
        >>> list(enumerate_with(['a', 'b', 'c']))
        [(0, 'a'), (1, 'b'), (2, 'c')]
    """
    return enumerate(iterable, start=start)


def zip_with(func: Callable[..., U], *iterables: Iterable[Any]) -> Iterable[U]:
    """
    Zip multiple iterables and apply function.

    Combines zip and map operations.

    Args:
        func: Function to apply to zipped elements
        *iterables: Multiple iterables to zip

    Returns:
        Iterator of function results

    Example:
        >>> a = [1, 2, 3]
        >>> b = [10, 20, 30]
        >>> list(zip_with(lambda x, y: x + y, a, b))
        [11, 22, 33]
    """
    return (func(*args) for args in zip(*iterables))


def pairwise(iterable: Iterable[T]) -> Iterable[tuple[T, T]]:
    """
    Create pairs of consecutive elements.

    Args:
        iterable: Source iterable

    Returns:
        Iterator of consecutive pairs

    Example:
        >>> list(pairwise([1, 2, 3, 4, 5]))
        [(1, 2), (2, 3), (3, 4), (4, 5)]
    """
    from itertools import tee

    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def sliding_window(iterable: Iterable[T], window_size: int) -> Iterable[tuple[T, ...]]:
    """
    Create sliding windows over iterable.

    Args:
        iterable: Source iterable
        window_size: Size of each window

    Returns:
        Iterator of windows (tuples)

    Example:
        >>> list(sliding_window([1, 2, 3, 4, 5], 3))
        [(1, 2, 3), (2, 3, 4), (3, 4, 5)]
    """
    from collections import deque
    from itertools import islice

    iterator = iter(iterable)
    window: deque[T] = deque(islice(iterator, window_size), maxlen=window_size)

    if len(window) == window_size:
        yield tuple(window)

    for item in iterator:
        window.append(item)
        yield tuple(window)


def batch(iterable: Iterable[T], batch_size: int) -> Iterable[list[T]]:
    """
    Batch elements into fixed-size groups.

    Args:
        iterable: Source iterable
        batch_size: Size of each batch

    Returns:
        Iterator of batches (lists)

    Example:
        >>> list(batch([1, 2, 3, 4, 5, 6, 7], 3))
        [[1, 2, 3], [4, 5, 6], [7]]
    """
    from itertools import islice

    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, batch_size))
        if not chunk:
            break
        yield chunk


def interleave(*iterables: Iterable[T]) -> Iterable[T]:
    """
    Interleave multiple iterables.

    Args:
        *iterables: Multiple iterables to interleave

    Returns:
        Iterator with interleaved elements

    Example:
        >>> list(interleave([1, 2, 3], ['a', 'b', 'c'], [10, 20, 30]))
        [1, 'a', 10, 2, 'b', 20, 3, 'c', 30]
    """
    from itertools import chain

    return chain.from_iterable(zip(*iterables))


def accumulate_with(
    iterable: Iterable[T], func: Callable[[U, T], U], initial: U
) -> Iterable[U]:
    """
    Accumulate values with custom function (scan operation).

    Similar to reduce but returns intermediate results.

    Args:
        iterable: Source iterable
        func: Accumulation function
        initial: Initial accumulator value

    Returns:
        Iterator of accumulated values

    Example:
        >>> list(accumulate_with([1, 2, 3, 4], lambda acc, x: acc + x, 0))
        [0, 1, 3, 6, 10]
    """
    from itertools import accumulate

    return accumulate(iterable, func, initial=initial)
