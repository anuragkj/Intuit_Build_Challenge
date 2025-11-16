"""
Unit Tests for Grouping Operations Module.

Tests group_by, partition, and related operations.
"""

from src.grouping import (
    count_by_key,
    group_and_aggregate,
    group_by,
    group_by_multiple,
    group_by_sorted,
    index_by,
    nested_group_by,
    partition_by,
)


def test_group_by() -> None:
    """Test basic grouping by key function."""
    data = [
        {"category": "A", "value": 100},
        {"category": "B", "value": 200},
        {"category": "A", "value": 150},
        {"category": "C", "value": 300},
        {"category": "B", "value": 250},
    ]

    result = group_by(data, lambda d: d["category"])

    assert len(result) == 3
    assert len(result["A"]) == 2
    assert len(result["B"]) == 2
    assert len(result["C"]) == 1


def test_group_by_empty() -> None:
    """Test grouping empty iterable."""
    result = group_by([], lambda d: d["category"])
    assert result == {}


def test_group_by_sorted() -> None:
    """Test grouping with sorting (using itertools.groupby)."""
    data = [
        {"category": "B", "value": 2},
        {"category": "A", "value": 1},
        {"category": "A", "value": 3},
        {"category": "C", "value": 5},
    ]

    result = group_by_sorted(data, lambda d: d["category"])

    assert len(result) == 3
    assert len(result["A"]) == 2
    assert len(result["B"]) == 1
    assert len(result["C"]) == 1


def test_group_by_multiple() -> None:
    """Test grouping by multiple keys (composite key)."""
    data = [
        {"region": "North", "category": "A", "value": 100},
        {"region": "North", "category": "B", "value": 200},
        {"region": "South", "category": "A", "value": 150},
        {"region": "North", "category": "A", "value": 120},
    ]

    result = group_by_multiple(data, lambda d: d["region"], lambda d: d["category"])

    assert ("North", "A") in result
    assert len(result[("North", "A")]) == 2
    assert ("North", "B") in result
    assert len(result[("North", "B")]) == 1
    assert ("South", "A") in result
    assert len(result[("South", "A")]) == 1


def test_nested_group_by() -> None:
    """Test nested (hierarchical) grouping."""
    data = [
        {"region": "North", "category": "A", "value": 100},
        {"region": "North", "category": "A", "value": 150},
        {"region": "North", "category": "B", "value": 200},
        {"region": "South", "category": "A", "value": 300},
    ]

    result = nested_group_by(data, lambda d: d["region"], lambda d: d["category"])

    assert "North" in result
    assert "South" in result
    assert "A" in result["North"]
    assert "B" in result["North"]
    assert len(result["North"]["A"]) == 2
    assert len(result["North"]["B"]) == 1
    assert len(result["South"]["A"]) == 1


def test_nested_group_by_single_level() -> None:
    """Test nested grouping with single key (behaves like group_by)."""
    data = [{"category": "A", "value": 1}, {"category": "B", "value": 2}]

    result = nested_group_by(data, lambda d: d["category"])

    assert "A" in result
    assert "B" in result
    assert len(result["A"]) == 1
    assert len(result["B"]) == 1


def test_partition_by() -> None:
    """Test partitioning by predicate."""
    data = [
        {"amount": 100, "has_discount": True},
        {"amount": 200, "has_discount": False},
        {"amount": 150, "has_discount": True},
        {"amount": 250, "has_discount": False},
    ]

    with_discount, without_discount = partition_by(data, lambda d: d["has_discount"])

    assert len(with_discount) == 2
    assert len(without_discount) == 2
    assert all(d["has_discount"] for d in with_discount)
    assert all(not d["has_discount"] for d in without_discount)


def test_index_by() -> None:
    """Test creating index (lookup table) by key."""
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]

    result = index_by(data, lambda d: d["id"])

    assert len(result) == 3
    assert result[1]["name"] == "Alice"
    assert result[2]["name"] == "Bob"
    assert result[3]["name"] == "Charlie"


def test_index_by_duplicate_keys() -> None:
    """Test index_by with duplicate keys (keeps last)."""
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 1, "name": "Alice Updated"},
    ]

    result = index_by(data, lambda d: d["id"])

    assert len(result) == 2
    assert result[1]["name"] == "Alice Updated"  # Last occurrence kept


def test_group_and_aggregate() -> None:
    """Test grouping with aggregation."""
    data = [
        {"category": "A", "amount": 100},
        {"category": "A", "amount": 150},
        {"category": "B", "amount": 200},
        {"category": "B", "amount": 250},
    ]

    result = group_and_aggregate(
        data,
        key_func=lambda d: d["category"],
        aggregate_func=lambda group: sum(d["amount"] for d in group),
    )

    assert result["A"] == 250  # 100 + 150
    assert result["B"] == 450  # 200 + 250


def test_count_by_key() -> None:
    """Test counting occurrences by key."""
    data = [
        {"category": "A"},
        {"category": "B"},
        {"category": "A"},
        {"category": "A"},
        {"category": "C"},
        {"category": "B"},
    ]

    result = count_by_key(data, lambda d: d["category"])

    assert result["A"] == 3
    assert result["B"] == 2
    assert result["C"] == 1
