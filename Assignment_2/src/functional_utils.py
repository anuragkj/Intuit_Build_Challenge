"""
Functional Programming Utilities.

Provides core functional programming utilities including:
- Function composition
- Currying and partial application
- Higher-order functions
- Functional combinators

These utilities enable elegant pipeline-style data processing.
"""

from collections.abc import Callable, Iterable
from functools import reduce
from typing import Any, TypeVar

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


def compose(*functions: Callable[..., Any]) -> Callable[..., Any]:
    """
    Compose functions right-to-left: compose(f, g, h)(x) = f(g(h(x))).

    Demonstrates functional composition, a core FP concept.
    Allows building complex transformations from simple functions.

    Args:
        *functions: Variable number of functions to compose

    Returns:
        Composed function

    Time Complexity: O(n) where n is number of functions
    Space Complexity: O(1)

    Example:
        >>> add_one = lambda x: x + 1
        >>> double = lambda x: x * 2
        >>> add_then_double = compose(double, add_one)
        >>> add_then_double(5)  # (5 + 1) * 2 = 12
        12
    """

    def composed(arg: Any) -> Any:
        return reduce(lambda result, func: func(result), reversed(functions), arg)

    return composed


def pipe(*functions: Callable[..., Any]) -> Callable[..., Any]:
    """
    Compose functions left-to-right: pipe(f, g, h)(x) = h(g(f(x))).

    More intuitive than compose for pipeline-style processing.

    Args:
        *functions: Variable number of functions to compose

    Returns:
        Composed function in forward order

    Example:
        >>> add_one = lambda x: x + 1
        >>> double = lambda x: x * 2
        >>> process = pipe(add_one, double)
        >>> process(5)  # (5 + 1) * 2 = 12
        12
    """

    def piped(arg: Any) -> Any:
        return reduce(lambda result, func: func(result), functions, arg)

    return piped


def curry2(func: Callable[[T, U], V]) -> Callable[[T], Callable[[U], V]]:
    """
    Curry a two-argument function.

    Transforms f(x, y) into f(x)(y) for partial application.

    Args:
        func: Function taking two arguments

    Returns:
        Curried function

    Example:
        >>> def add(x, y): return x + y
        >>> add_five = curry2(add)(5)
        >>> add_five(3)
        8
    """

    def curried(x: T) -> Callable[[U], V]:
        def inner(y: U) -> V:
            return func(x, y)

        return inner

    return curried


def identity(x: T) -> T:
    """
    Identity function: returns input unchanged.

    Useful as default function in higher-order functions.

    Args:
        x: Any value

    Returns:
        The same value

    Example:
        >>> identity(5)
        5
        >>> list(map(identity, [1, 2, 3]))
        [1, 2, 3]
    """
    return x


def const(value: T) -> Callable[..., T]:
    """
    Create constant function that always returns the same value.

    Args:
        value: Value to return

    Returns:
        Function that ignores arguments and returns value

    Example:
        >>> always_five = const(5)
        >>> always_five()
        5
        >>> always_five(1, 2, 3)
        5
    """

    def constant(*args: Any, **kwargs: Any) -> T:
        return value

    return constant


def flip(func: Callable[[T, U], V]) -> Callable[[U, T], V]:
    """
    Flip the order of a two-argument function's parameters.

    Args:
        func: Function taking two arguments

    Returns:
        Function with flipped parameters

    Example:
        >>> def divide(x, y): return x / y
        >>> flipped_divide = flip(divide)
        >>> flipped_divide(2, 10)  # 10 / 2
        5.0
    """

    def flipped(y: U, x: T) -> V:
        return func(x, y)

    return flipped


def take(n: int, iterable: Iterable[T]) -> list[T]:
    """
    Take first n elements from iterable.

    Args:
        n: Number of elements to take
        iterable: Source iterable

    Returns:
        List of first n elements

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> take(3, [1, 2, 3, 4, 5])
        [1, 2, 3]
    """
    from itertools import islice

    return list(islice(iterable, n))


def drop(n: int, iterable: Iterable[T]) -> Iterable[T]:
    """
    Drop first n elements from iterable.

    Args:
        n: Number of elements to drop
        iterable: Source iterable

    Returns:
        Iterator over remaining elements

    Time Complexity: O(n) to drop, O(1) per remaining element
    Space Complexity: O(1)

    Example:
        >>> list(drop(2, [1, 2, 3, 4, 5]))
        [3, 4, 5]
    """
    from itertools import islice

    return islice(iterable, n, None)


def flatten(nested: Iterable[Iterable[T]]) -> Iterable[T]:
    """
    Flatten one level of nesting.

    Args:
        nested: Nested iterable

    Returns:
        Flattened iterator

    Example:
        >>> list(flatten([[1, 2], [3, 4], [5]]))
        [1, 2, 3, 4, 5]
    """
    from itertools import chain

    return chain.from_iterable(nested)


def unique(iterable: Iterable[T]) -> list[T]:
    """
    Get unique elements while preserving order.

    Args:
        iterable: Source iterable

    Returns:
        List of unique elements in order of first appearance

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> unique([1, 2, 2, 3, 1, 4])
        [1, 2, 3, 4]
    """
    seen: set[T] = set()
    result: list[T] = []
    for item in iterable:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def partition(
    predicate: Callable[[T], bool], iterable: Iterable[T]
) -> tuple[list[T], list[T]]:
    """
    Partition iterable into two lists based on predicate.

    Args:
        predicate: Boolean function to test each element
        iterable: Source iterable

    Returns:
        Tuple of (elements matching predicate, elements not matching)

    Time Complexity: O(n)
    Space Complexity: O(n)

    Example:
        >>> is_even = lambda x: x % 2 == 0
        >>> partition(is_even, [1, 2, 3, 4, 5])
        ([2, 4], [1, 3, 5])
    """
    true_items: list[T] = []
    false_items: list[T] = []

    for item in iterable:
        if predicate(item):
            true_items.append(item)
        else:
            false_items.append(item)

    return true_items, false_items


def chunk(iterable: Iterable[T], size: int) -> Iterable[list[T]]:
    """
    Split iterable into chunks of given size.

    Args:
        iterable: Source iterable
        size: Chunk size

    Yields:
        Lists of size elements (last chunk may be smaller)

    Example:
        >>> list(chunk([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    iterator = iter(iterable)
    while True:
        batch = take(size, iterator)
        if not batch:
            break
        yield batch


def apply_to_pairs(func: Callable[[T, T], U], iterable: Iterable[T]) -> Iterable[U]:
    """
    Apply function to consecutive pairs of elements.

    Args:
        func: Binary function to apply
        iterable: Source iterable

    Yields:
        Results of applying func to consecutive pairs

    Example:
        >>> subtract = lambda x, y: x - y
        >>> list(apply_to_pairs(subtract, [5, 3, 2, 1]))
        [2, 1, 1]  # 5-3, 3-2, 2-1
    """
    from itertools import tee

    a, b = tee(iterable)
    next(b, None)  # Advance second iterator by one
    return (func(x, y) for x, y in zip(a, b))


def pairwise(iterable: Iterable[T]) -> Iterable[tuple[T, T]]:
    """
    Return successive overlapping pairs from iterable.

    Args:
        iterable: Source iterable

    Yields:
        Tuples of consecutive pairs

    Example:
        >>> list(pairwise([1, 2, 3, 4, 5]))
        [(1, 2), (2, 3), (3, 4), (4, 5)]
    """
    from itertools import tee

    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def sliding_window(iterable: Iterable[T], size: int) -> Iterable[tuple[T, ...]]:
    """
    Create sliding windows of given size over iterable.

    Args:
        iterable: Source iterable
        size: Window size

    Yields:
        Tuples representing sliding windows

    Example:
        >>> list(sliding_window([1, 2, 3, 4, 5], 3))
        [(1, 2, 3), (2, 3, 4), (3, 4, 5)]
    """
    from collections import deque
    from itertools import islice

    it = iter(iterable)
    window: deque[T] = deque(islice(it, size), maxlen=size)
    if len(window) == size:
        yield tuple(window)
    for item in it:
        window.append(item)
        yield tuple(window)


def batch(iterable: Iterable[T], size: int) -> Iterable[list[T]]:
    """
    Batch iterable into lists of given size (alias for chunk).

    Args:
        iterable: Source iterable
        size: Batch size

    Yields:
        Lists of size elements (last batch may be smaller)

    Example:
        >>> list(batch([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    return chunk(iterable, size)


def interleave(*iterables: Iterable[T]) -> Iterable[T]:
    """
    Interleave multiple iterables element by element.

    Args:
        *iterables: Variable number of iterables

    Yields:
        Elements from all iterables in round-robin fashion

    Example:
        >>> list(interleave([1, 2, 3], ['a', 'b', 'c'], [10, 20, 30]))
        [1, 'a', 10, 2, 'b', 20, 3, 'c', 30]
    """
    from itertools import chain

    return chain.from_iterable(zip(*iterables))


def zip_with(func: Callable[..., U], *iterables: Iterable[Any]) -> Iterable[U]:
    """
    Zip iterables and apply function to each tuple of elements.

    Args:
        func: Function to apply to zipped elements
        *iterables: Variable number of iterables

    Yields:
        Results of applying func to zipped elements

    Example:
        >>> list(zip_with(lambda x, y: x + y, [1, 2, 3], [10, 20, 30]))
        [11, 22, 33]
    """
    return (func(*args) for args in zip(*iterables))


def accumulate_with(
    iterable: Iterable[T], func: Callable[[U, T], U], initial: U
) -> Iterable[U]:
    """
    Accumulate values using a custom function with initial value.

    Similar to functools.reduce but yields intermediate results.

    Args:
        iterable: Source iterable
        func: Binary function for accumulation
        initial: Initial accumulator value

    Yields:
        Intermediate accumulation results (including initial)

    Example:
        >>> list(accumulate_with([1, 2, 3, 4], lambda acc, x: acc + x, 0))
        [0, 1, 3, 6, 10]
    """
    accumulator = initial
    yield accumulator
    for item in iterable:
        accumulator = func(accumulator, item)
        yield accumulator
