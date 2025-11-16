"""
Unit Tests for Transformation Operations Module.

Tests mapping, extraction, projection, and transformation operations.
"""

from src.transformations import (
    accumulate_with,
    add_computed_field,
    batch,
    enumerate_with,
    extract_field,
    extract_fields,
    flatmap,
    interleave,
    map_by,
    pairwise,
    project,
    sliding_window,
    zip_with,
)


def test_map_by() -> None:
    """Test basic mapping transformation."""
    data = [1, 2, 3, 4, 5]
    result = list(map_by(data, lambda x: x * 2))
    assert result == [2, 4, 6, 8, 10]


def test_map_by_empty() -> None:
    """Test mapping empty iterable."""
    result = list(map_by([], lambda x: x * 2))
    assert result == []


def test_extract_field() -> None:
    """Test extracting single field."""
    transactions = [{"amount": 100}, {"amount": 200}, {"amount": 300}]
    result = list(extract_field(transactions, lambda t: t["amount"]))
    assert result == [100, 200, 300]


def test_extract_field_with_transformation() -> None:
    """Test extracting field with transformation."""
    transactions = [{"amount": 100}, {"amount": 200}]
    result = list(extract_field(transactions, lambda t: t["amount"] * 2))
    assert result == [200, 400]


def test_extract_fields() -> None:
    """Test extracting multiple fields."""
    transactions = [
        {"id": 1, "amount": 100, "category": "A"},
        {"id": 2, "amount": 200, "category": "B"},
    ]
    result = list(
        extract_fields(transactions, lambda t: t["id"], lambda t: t["amount"])
    )
    assert result == [(1, 100), (2, 200)]


def test_extract_fields_single() -> None:
    """Test extract_fields with single field."""
    data = [{"x": 1}, {"x": 2}, {"x": 3}]
    result = list(extract_fields(data, lambda d: d["x"]))
    assert result == [(1,), (2,), (3,)]


def test_project() -> None:
    """Test projecting specific fields from dictionaries."""
    transactions = [
        {"id": 1, "amount": 100, "category": "A", "date": "2023-01-01"},
        {"id": 2, "amount": 200, "category": "B", "date": "2023-01-02"},
    ]
    result = list(project(transactions, "id", "amount"))
    assert result == [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}]


def test_project_missing_fields() -> None:
    """Test projection when some fields are missing."""
    data = [{"id": 1, "amount": 100}, {"id": 2}]
    result = list(project(data, "id", "amount"))
    assert result == [{"id": 1, "amount": 100}, {"id": 2}]


def test_add_computed_field() -> None:
    """Test adding computed field to dictionaries."""
    transactions = [
        {"price": 100, "quantity": 2},
        {"price": 50, "quantity": 3},
    ]
    result = list(
        add_computed_field(transactions, "total", lambda t: t["price"] * t["quantity"])
    )
    assert result == [
        {"price": 100, "quantity": 2, "total": 200},
        {"price": 50, "quantity": 3, "total": 150},
    ]


def test_add_computed_field_immutability() -> None:
    """Test that add_computed_field doesn't mutate original."""
    original = [{"price": 100, "quantity": 2}]
    list(add_computed_field(original, "total", lambda t: t["price"] * t["quantity"]))
    # Original should not have 'total' field
    assert "total" not in original[0]


def test_flatmap() -> None:
    """Test flatmap operation."""
    data = [[1, 2], [3, 4], [5]]
    result = list(flatmap(data, lambda x: x))
    assert result == [1, 2, 3, 4, 5]


def test_flatmap_with_transformation() -> None:
    """Test flatmap with transformation."""
    words = ["hello", "world"]
    result = list(flatmap(words, lambda w: list(w)))
    assert result == ["h", "e", "l", "l", "o", "w", "o", "r", "l", "d"]


def test_flatmap_empty() -> None:
    """Test flatmap with empty iterables."""
    data = [[], [1], [], [2, 3]]
    result = list(flatmap(data, lambda x: x))
    assert result == [1, 2, 3]


def test_enumerate_with() -> None:
    """Test enumeration with default start."""
    data = ["a", "b", "c"]
    result = list(enumerate_with(data))
    assert result == [(0, "a"), (1, "b"), (2, "c")]


def test_enumerate_with_custom_start() -> None:
    """Test enumeration with custom start index."""
    data = ["a", "b", "c"]
    result = list(enumerate_with(data, start=1))
    assert result == [(1, "a"), (2, "b"), (3, "c")]


def test_zip_with() -> None:
    """Test zipping with function."""
    a = [1, 2, 3]
    b = [10, 20, 30]
    result = list(zip_with(lambda x, y: x + y, a, b))
    assert result == [11, 22, 33]


def test_zip_with_multiple_iterables() -> None:
    """Test zip_with with more than two iterables."""
    a = [1, 2, 3]
    b = [10, 20, 30]
    c = [100, 200, 300]
    result = list(zip_with(lambda x, y, z: x + y + z, a, b, c))
    assert result == [111, 222, 333]


def test_zip_with_unequal_lengths() -> None:
    """Test zip_with with different length iterables."""
    a = [1, 2, 3]
    b = [10, 20]
    result = list(zip_with(lambda x, y: x + y, a, b))
    assert result == [11, 22]  # Stops at shortest


def test_pairwise() -> None:
    """Test creating consecutive pairs."""
    data = [1, 2, 3, 4, 5]
    result = list(pairwise(data))
    assert result == [(1, 2), (2, 3), (3, 4), (4, 5)]


def test_pairwise_two_elements() -> None:
    """Test pairwise with exactly two elements."""
    data = [1, 2]
    result = list(pairwise(data))
    assert result == [(1, 2)]


def test_pairwise_single_element() -> None:
    """Test pairwise with single element."""
    data = [1]
    result = list(pairwise(data))
    assert result == []


def test_sliding_window() -> None:
    """Test sliding window operation."""
    data = [1, 2, 3, 4, 5]
    result = list(sliding_window(data, 3))
    assert result == [(1, 2, 3), (2, 3, 4), (3, 4, 5)]


def test_sliding_window_size_two() -> None:
    """Test sliding window with size 2 (same as pairwise)."""
    data = [1, 2, 3, 4]
    result = list(sliding_window(data, 2))
    assert result == [(1, 2), (2, 3), (3, 4)]


def test_sliding_window_larger_than_data() -> None:
    """Test sliding window when window size > data size."""
    data = [1, 2]
    result = list(sliding_window(data, 5))
    assert result == []


def test_batch() -> None:
    """Test batching elements."""
    data = [1, 2, 3, 4, 5, 6, 7]
    result = list(batch(data, 3))
    assert result == [[1, 2, 3], [4, 5, 6], [7]]


def test_batch_exact_fit() -> None:
    """Test batching when size divides evenly."""
    data = [1, 2, 3, 4, 5, 6]
    result = list(batch(data, 2))
    assert result == [[1, 2], [3, 4], [5, 6]]


def test_batch_size_one() -> None:
    """Test batching with size 1."""
    data = [1, 2, 3]
    result = list(batch(data, 1))
    assert result == [[1], [2], [3]]


def test_interleave() -> None:
    """Test interleaving multiple iterables."""
    a = [1, 2, 3]
    b = ["a", "b", "c"]
    c = [10, 20, 30]
    result = list(interleave(a, b, c))
    assert result == [1, "a", 10, 2, "b", 20, 3, "c", 30]


def test_interleave_two_iterables() -> None:
    """Test interleaving two iterables."""
    a = [1, 2, 3]
    b = ["a", "b", "c"]
    result = list(interleave(a, b))
    assert result == [1, "a", 2, "b", 3, "c"]


def test_interleave_unequal_lengths() -> None:
    """Test interleave with unequal length iterables."""
    a = [1, 2]
    b = ["a", "b", "c"]
    result = list(interleave(a, b))
    # Stops at shortest iterable
    assert result == [1, "a", 2, "b"]


def test_accumulate_with() -> None:
    """Test accumulation with custom function."""
    data = [1, 2, 3, 4]
    result = list(accumulate_with(data, lambda acc, x: acc + x, 0))
    assert result == [0, 1, 3, 6, 10]


def test_accumulate_with_multiplication() -> None:
    """Test accumulation with multiplication."""
    data = [2, 3, 4]
    result = list(accumulate_with(data, lambda acc, x: acc * x, 1))
    assert result == [1, 2, 6, 24]


def test_accumulate_with_empty() -> None:
    """Test accumulate with empty iterable."""
    result = list(accumulate_with([], lambda acc, x: acc + x, 0))
    assert result == [0]


def test_complex_pipeline() -> None:
    """Test complex transformation pipeline."""
    # Extract amounts, double them, batch into groups
    transactions = [{"amount": 100}, {"amount": 200}, {"amount": 300}, {"amount": 400}]

    # Pipeline: extract → map → batch
    amounts = extract_field(transactions, lambda t: t["amount"])
    doubled = map_by(amounts, lambda x: x * 2)
    batched = list(batch(doubled, 2))

    assert batched == [[200, 400], [600, 800]]
