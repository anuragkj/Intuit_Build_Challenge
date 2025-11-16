"""
Unit Tests for Functional Utilities Module.

Tests function composition, currying, and helper functions.
"""

from src.functional_utils import (
    accumulate_with,
    apply_to_pairs,
    batch,
    chunk,
    compose,
    const,
    drop,
    flatten,
    flip,
    identity,
    interleave,
    pairwise,
    partition,
    pipe,
    sliding_window,
    take,
    unique,
    zip_with,
)


def test_compose() -> None:
    """Test function composition (right-to-left)."""
    add_one = lambda x: x + 1
    double = lambda x: x * 2

    # compose(double, add_one)(5) = double(add_one(5)) = double(6) = 12
    composed = compose(double, add_one)
    assert composed(5) == 12


def test_compose_multiple() -> None:
    """Test composing multiple functions."""
    add_one = lambda x: x + 1
    double = lambda x: x * 2
    square = lambda x: x * x

    # compose(square, double, add_one)(3) = square(double(add_one(3))) = square(double(4)) = square(8) = 64
    composed = compose(square, double, add_one)
    assert composed(3) == 64


def test_pipe() -> None:
    """Test function composition (left-to-right)."""
    add_one = lambda x: x + 1
    double = lambda x: x * 2

    # pipe(add_one, double)(5) = double(add_one(5)) = double(6) = 12
    piped = pipe(add_one, double)
    assert piped(5) == 12


def test_identity() -> None:
    """Test identity function."""
    assert identity(5) == 5
    assert identity("hello") == "hello"
    assert identity([1, 2, 3]) == [1, 2, 3]


def test_const() -> None:
    """Test constant function."""
    always_five = const(5)
    assert always_five() == 5
    assert always_five(1, 2, 3) == 5
    assert always_five(x=10, y=20) == 5


def test_flip() -> None:
    """Test flipping function arguments."""

    def subtract(x: int, y: int) -> int:
        return x - y

    flipped_subtract = flip(subtract)

    assert subtract(10, 3) == 7
    assert flipped_subtract(3, 10) == 7


def test_take() -> None:
    """Test taking first n elements."""
    assert take(3, [1, 2, 3, 4, 5]) == [1, 2, 3]
    assert take(2, [1]) == [1]
    assert take(5, [1, 2]) == [1, 2]
    assert take(0, [1, 2, 3]) == []


def test_drop() -> None:
    """Test dropping first n elements."""
    assert list(drop(2, [1, 2, 3, 4, 5])) == [3, 4, 5]
    assert list(drop(1, [1])) == []
    assert list(drop(0, [1, 2, 3])) == [1, 2, 3]
    assert list(drop(5, [1, 2])) == []


def test_flatten() -> None:
    """Test flattening nested iterables."""
    assert list(flatten([[1, 2], [3, 4], [5]])) == [1, 2, 3, 4, 5]
    assert list(flatten([[], [1], [], [2, 3]])) == [1, 2, 3]
    assert list(flatten([[]])) == []


def test_unique() -> None:
    """Test getting unique elements."""
    assert unique([1, 2, 2, 3, 1, 4]) == [1, 2, 3, 4]
    assert unique([1, 1, 1]) == [1]
    assert unique([]) == []
    assert unique([1, 2, 3]) == [1, 2, 3]


def test_partition() -> None:
    """Test partitioning by predicate."""
    is_even = lambda x: x % 2 == 0
    evens, odds = partition(is_even, [1, 2, 3, 4, 5, 6])

    assert evens == [2, 4, 6]
    assert odds == [1, 3, 5]


def test_partition_all_true() -> None:
    """Test partition when all elements match."""
    is_positive = lambda x: x > 0
    positive, negative = partition(is_positive, [1, 2, 3, 4])

    assert positive == [1, 2, 3, 4]
    assert negative == []


def test_partition_all_false() -> None:
    """Test partition when no elements match."""
    is_negative = lambda x: x < 0
    negative, non_negative = partition(is_negative, [1, 2, 3, 4])

    assert negative == []
    assert non_negative == [1, 2, 3, 4]


def test_chunk() -> None:
    """Test chunking iterable."""
    result = list(chunk([1, 2, 3, 4, 5, 6, 7], 3))
    assert result == [[1, 2, 3], [4, 5, 6], [7]]


def test_chunk_exact_fit() -> None:
    """Test chunking when size divides evenly."""
    result = list(chunk([1, 2, 3, 4, 5, 6], 2))
    assert result == [[1, 2], [3, 4], [5, 6]]


def test_apply_to_pairs() -> None:
    """Test applying function to consecutive pairs."""
    subtract = lambda x, y: x - y
    result = list(apply_to_pairs(subtract, [5, 3, 2, 1]))
    assert result == [2, 1, 1]  # 5-3, 3-2, 2-1


def test_apply_to_pairs_single_element() -> None:
    """Test apply_to_pairs with single element."""
    add = lambda x, y: x + y
    result = list(apply_to_pairs(add, [5]))
    assert result == []


def test_pairwise() -> None:
    """Test creating consecutive pairs."""
    result = list(pairwise([1, 2, 3, 4, 5]))
    assert result == [(1, 2), (2, 3), (3, 4), (4, 5)]


def test_sliding_window() -> None:
    """Test sliding window."""
    result = list(sliding_window([1, 2, 3, 4, 5], 3))
    assert result == [(1, 2, 3), (2, 3, 4), (3, 4, 5)]


def test_sliding_window_size_larger_than_list() -> None:
    """Test sliding window when window size > list size."""
    result = list(sliding_window([1, 2], 5))
    assert result == []


def test_batch() -> None:
    """Test batching elements."""
    result = list(batch([1, 2, 3, 4, 5, 6, 7], 3))
    assert result == [[1, 2, 3], [4, 5, 6], [7]]


def test_interleave() -> None:
    """Test interleaving iterables."""
    result = list(interleave([1, 2, 3], ["a", "b", "c"], [10, 20, 30]))
    assert result == [1, "a", 10, 2, "b", 20, 3, "c", 30]


def test_zip_with() -> None:
    """Test zip with function."""
    result = list(zip_with(lambda x, y: x + y, [1, 2, 3], [10, 20, 30]))
    assert result == [11, 22, 33]


def test_accumulate_with() -> None:
    """Test accumulation with custom function."""
    result = list(accumulate_with([1, 2, 3, 4], lambda acc, x: acc + x, 0))
    assert result == [0, 1, 3, 6, 10]


def test_accumulate_with_multiply() -> None:
    """Test accumulation with multiplication."""
    result = list(accumulate_with([2, 3, 4], lambda acc, x: acc * x, 1))
    assert result == [1, 2, 6, 24]
