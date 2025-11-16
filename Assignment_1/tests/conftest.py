"""
Pytest configuration and shared fixtures.

This module provides reusable test fixtures for creating queue instances,
containers, and configuration objects used across the test suite.
"""

import queue
from typing import Any, List

import pytest

from src.config import ConsumerConfig, CoordinatorConfig, ProducerConfig
from src.custom_queue import ThreadSafeQueue


@pytest.fixture
def custom_queue() -> ThreadSafeQueue[Any]:
    """
    Create a custom ThreadSafeQueue with default capacity.

    Returns:
        ThreadSafeQueue instance with capacity of 10
    """
    return ThreadSafeQueue(maxsize=10)


@pytest.fixture
def custom_queue_unbounded() -> ThreadSafeQueue[Any]:
    """
    Create an unbounded custom ThreadSafeQueue.

    Returns:
        ThreadSafeQueue instance with no size limit
    """
    return ThreadSafeQueue(maxsize=0)


@pytest.fixture
def stdlib_queue() -> queue.Queue[Any]:
    """
    Create a standard library Queue with default capacity.

    Returns:
        queue.Queue instance with capacity of 10
    """
    return queue.Queue(maxsize=10)


@pytest.fixture
def stdlib_queue_unbounded() -> queue.Queue[Any]:
    """
    Create an unbounded standard library Queue.

    Returns:
        queue.Queue instance with no size limit
    """
    return queue.Queue(maxsize=0)


@pytest.fixture
def source_container() -> List[str]:
    """
    Create a source container with test data.

    Returns:
        List of string items for testing
    """
    return [f"item_{i}" for i in range(20)]


@pytest.fixture
def large_source_container() -> List[str]:
    """
    Create a large source container for stress testing.

    Returns:
        List of 1000 string items
    """
    return [f"item_{i}" for i in range(1000)]


@pytest.fixture
def destination_container() -> List[Any]:
    """
    Create an empty destination container.

    Returns:
        Empty list for storing consumed items
    """
    return []


@pytest.fixture
def producer_config() -> ProducerConfig:
    """
    Create a default ProducerConfig for testing.

    Returns:
        ProducerConfig with test-friendly settings
    """
    return ProducerConfig(
        name="TestProducer",
        put_timeout=5.0,
        delay_between_items=0.0,
        stop_on_error=True,
    )


@pytest.fixture
def consumer_config() -> ConsumerConfig:
    """
    Create a default ConsumerConfig for testing.

    Returns:
        ConsumerConfig with test-friendly settings
    """
    return ConsumerConfig(
        name="TestConsumer",
        get_timeout=5.0,
        delay_between_items=0.0,
        stop_on_error=True,
    )


@pytest.fixture
def coordinator_config() -> CoordinatorConfig:
    """
    Create a default CoordinatorConfig for testing.

    Returns:
        CoordinatorConfig with test-friendly settings
    """
    return CoordinatorConfig(
        name="TestCoordinator",
        join_timeout=10.0,
        sentinel_value=None,
        shutdown_grace_period=1.0,
    )


@pytest.fixture
def sentinel_value() -> None:
    """
    Create a sentinel value for testing.

    Returns:
        None as the sentinel value
    """
    return None
