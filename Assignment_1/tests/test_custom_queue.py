"""
Comprehensive tests for the custom ThreadSafeQueue implementation.

This test module validates all aspects of the custom queue including:
- Basic operations (put, get, qsize, empty, full)
- Blocking behavior with proper wait/notify
- Timeout handling
- Thread safety with concurrent access
- Edge cases (unbounded, single capacity, etc.)
"""

import time
from threading import Thread
from typing import Any, List

import pytest

from src.custom_queue import ThreadSafeQueue
from src.exceptions import QueueEmpty, QueueFull


class TestBasicOperations:
    """Test basic queue operations without concurrency."""

    def test_put_and_get_single_item(self) -> None:
        """Test putting and getting a single item."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)
        q.put("test_item")
        item = q.get()
        assert item == "test_item"
        assert q.empty()

    def test_put_and_get_multiple_items(self) -> None:
        """Test FIFO ordering with multiple items."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=10)
        items = [1, 2, 3, 4, 5]

        for item in items:
            q.put(item)

        retrieved = [q.get() for _ in range(len(items))]
        assert retrieved == items

    def test_qsize(self) -> None:
        """Test qsize() returns correct queue size."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)
        assert q.qsize() == 0

        q.put("item1")
        assert q.qsize() == 1

        q.put("item2")
        assert q.qsize() == 2

        q.get()
        assert q.qsize() == 1

        q.get()
        assert q.qsize() == 0

    def test_empty(self) -> None:
        """Test empty() returns correct status."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)
        assert q.empty()

        q.put("item")
        assert not q.empty()

        q.get()
        assert q.empty()

    def test_full_bounded_queue(self) -> None:
        """Test full() for bounded queue."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=3)
        assert not q.full()

        q.put(1)
        assert not q.full()

        q.put(2)
        assert not q.full()

        q.put(3)
        assert q.full()

        q.get()
        assert not q.full()

    def test_full_unbounded_queue(self) -> None:
        """Test full() always returns False for unbounded queue."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=0)
        assert not q.full()

        for i in range(100):
            q.put(i)
            assert not q.full()


class TestNonBlockingBehavior:
    """Test non-blocking put and get operations."""

    def test_get_empty_queue_non_blocking(self) -> None:
        """Test get() raises QueueEmpty when queue is empty and block=False."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)

        with pytest.raises(QueueEmpty):
            q.get(block=False)

    def test_put_full_queue_non_blocking(self) -> None:
        """Test put() raises QueueFull when queue is full and block=False."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=2)
        q.put(1)
        q.put(2)

        with pytest.raises(QueueFull):
            q.put(3, block=False)


class TestBlockingBehavior:
    """Test blocking put and get operations with timeouts."""

    def test_get_blocks_until_item_available(self) -> None:
        """Test get() blocks until item is put by another thread."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)
        result: List[str] = []

        def producer() -> None:
            time.sleep(0.2)  # Delay before producing
            q.put("delayed_item")

        def consumer() -> None:
            item = q.get(block=True)  # Should block until item available
            result.append(item)

        producer_thread = Thread(target=producer)
        consumer_thread = Thread(target=consumer)

        start_time = time.time()
        consumer_thread.start()
        producer_thread.start()

        consumer_thread.join(timeout=2.0)
        producer_thread.join(timeout=2.0)
        elapsed = time.time() - start_time

        assert result == ["delayed_item"]
        assert elapsed >= 0.2  # Verify it actually blocked

    def test_put_blocks_until_space_available(self) -> None:
        """Test put() blocks when queue is full until space is available."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=2)
        q.put(1)
        q.put(2)  # Queue is now full

        produced: List[int] = []

        def producer() -> None:
            q.put(3, block=True)  # Should block until space available
            produced.append(3)

        def consumer() -> None:
            time.sleep(0.2)  # Delay before consuming
            q.get()  # Make space

        producer_thread = Thread(target=producer)
        consumer_thread = Thread(target=consumer)

        start_time = time.time()
        producer_thread.start()
        consumer_thread.start()

        producer_thread.join(timeout=2.0)
        consumer_thread.join(timeout=2.0)
        elapsed = time.time() - start_time

        assert produced == [3]
        assert elapsed >= 0.2  # Verify it actually blocked

    def test_get_timeout(self) -> None:
        """Test get() raises QueueEmpty after timeout expires."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)

        start_time = time.time()
        with pytest.raises(QueueEmpty):
            q.get(block=True, timeout=0.3)
        elapsed = time.time() - start_time

        assert 0.25 <= elapsed <= 0.5  # Verify timeout occurred

    def test_put_timeout(self) -> None:
        """Test put() raises QueueFull after timeout expires."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=1)
        q.put(1)  # Fill the queue

        start_time = time.time()
        with pytest.raises(QueueFull):
            q.put(2, block=True, timeout=0.3)
        elapsed = time.time() - start_time

        assert 0.25 <= elapsed <= 0.5  # Verify timeout occurred

    def test_negative_timeout_raises_error(self) -> None:
        """Test that negative timeout raises ValueError."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)

        with pytest.raises(ValueError):
            q.get(block=True, timeout=-1.0)

        with pytest.raises(ValueError):
            q.put("item", block=True, timeout=-1.0)


class TestConcurrentAccess:
    """Test thread safety with concurrent producers and consumers."""

    def test_multiple_concurrent_producers(self) -> None:
        """Test multiple producers putting items concurrently."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=100)
        num_producers = 5
        items_per_producer = 20

        def producer(producer_id: int) -> None:
            for i in range(items_per_producer):
                q.put(f"producer_{producer_id}_item_{i}")

        threads = [Thread(target=producer, args=(i,)) for i in range(num_producers)]

        for t in threads:
            t.start()

        for t in threads:
            t.join(timeout=5.0)

        # Verify all items were added
        assert q.qsize() == num_producers * items_per_producer

        # Verify no data corruption (all items are retrievable)
        items = [q.get() for _ in range(q.qsize())]
        assert len(items) == num_producers * items_per_producer

    def test_multiple_concurrent_consumers(self) -> None:
        """Test multiple consumers getting items concurrently."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=100)
        num_items = 100
        num_consumers = 5

        # Pre-fill queue
        for i in range(num_items):
            q.put(i)

        consumed_items: List[int] = []
        lock = __import__("threading").Lock()

        def consumer() -> None:
            while True:
                try:
                    item = q.get(block=False)
                    with lock:
                        consumed_items.append(item)
                except QueueEmpty:
                    break

        threads = [Thread(target=consumer) for _ in range(num_consumers)]

        for t in threads:
            t.start()

        for t in threads:
            t.join(timeout=5.0)

        # Verify all items were consumed
        assert len(consumed_items) == num_items
        assert sorted(consumed_items) == list(range(num_items))

    def test_concurrent_producers_and_consumers(self) -> None:
        """Test producers and consumers working concurrently."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=10)
        num_items = 100
        produced: List[int] = []
        consumed: List[int] = []
        lock = __import__("threading").Lock()

        def producer() -> None:
            for i in range(num_items):
                q.put(i)
                with lock:
                    produced.append(i)

        def consumer() -> None:
            for _ in range(num_items):
                item = q.get(block=True, timeout=5.0)
                with lock:
                    consumed.append(item)

        producer_thread = Thread(target=producer)
        consumer_thread = Thread(target=consumer)

        producer_thread.start()
        consumer_thread.start()

        producer_thread.join(timeout=10.0)
        consumer_thread.join(timeout=10.0)

        # Verify all items produced and consumed
        assert len(produced) == num_items
        assert len(consumed) == num_items
        assert consumed == produced


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_unbounded_queue(self) -> None:
        """Test unbounded queue accepts unlimited items."""
        q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=0)

        # Add many items without blocking
        for i in range(1000):
            q.put(i, block=False)

        assert q.qsize() == 1000
        assert not q.full()

    def test_single_capacity_queue(self) -> None:
        """Test queue with capacity of 1."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=1)

        q.put("item1")
        assert q.full()

        with pytest.raises(QueueFull):
            q.put("item2", block=False)

        item = q.get()
        assert item == "item1"
        assert q.empty()

    def test_zero_timeout(self) -> None:
        """Test zero timeout behaves like non-blocking."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=1)

        # Empty queue
        with pytest.raises(QueueEmpty):
            q.get(block=True, timeout=0.0)

        # Full queue
        q.put("item")
        with pytest.raises(QueueFull):
            q.put("item2", block=True, timeout=0.0)

    def test_repr(self) -> None:
        """Test __repr__ returns useful string representation."""
        q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)
        repr_str = repr(q)

        assert "ThreadSafeQueue" in repr_str
        assert "maxsize=10" in repr_str
        assert "current_size=0" in repr_str

        q.put("item")
        repr_str = repr(q)
        assert "current_size=1" in repr_str
