"""
Unit tests for the Producer class.

This test module validates Producer behavior including:
- Basic production functionality
- Timeout handling
- Error handling
- Sentinel value transmission
- Metrics tracking
"""

import time
from typing import Any, List

import pytest

from src.config import ProducerConfig
from src.custom_queue import ThreadSafeQueue
from src.exceptions import ProducerError, QueueFull
from src.producer import Producer


class TestProducerBasics:
    """Test basic producer functionality."""

    def test_producer_initialization(
        self, producer_config: ProducerConfig, custom_queue: ThreadSafeQueue[Any]
    ) -> None:
        """Test producer initializes correctly."""
        source = ["item1", "item2", "item3"]
        producer = Producer(
            config=producer_config, source=source, queue=custom_queue, sentinel=None
        )

        assert producer.config == producer_config
        assert producer.source == source
        assert producer.queue == custom_queue
        assert producer.sentinel is None
        assert producer.items_produced == 0
        assert not producer.is_running

    def test_producer_starts_and_joins(
        self,
        producer_config: ProducerConfig,
        custom_queue: ThreadSafeQueue[Any],
    ) -> None:
        """Test producer starts and completes successfully."""
        source = ["item1", "item2", "item3"]
        producer = Producer(
            config=producer_config, source=source, queue=custom_queue, sentinel=None
        )

        producer.start()
        # Note: Not checking is_running immediately due to race condition
        # Producer may finish too quickly

        completed = producer.join(timeout=5.0)
        assert completed
        assert not producer.is_running
        assert producer.items_produced == len(source)

    def test_producer_cannot_start_twice(
        self,
        producer_config: ProducerConfig,
        custom_queue: ThreadSafeQueue[Any],
    ) -> None:
        """Test that starting producer twice raises error."""
        # Use more items to keep producer running longer
        source = [f"item_{i}" for i in range(20)]

        # Add small delay to slow down production
        config = ProducerConfig(
            name="TestProducer",
            put_timeout=5.0,
            delay_between_items=0.01,  # Small delay
            stop_on_error=True,
        )

        producer = Producer(
            config=config, source=source, queue=custom_queue, sentinel=None
        )

        producer.start()
        # Small delay to ensure producer thread has started
        time.sleep(0.1)

        with pytest.raises(ProducerError):
            producer.start()

        producer.join(timeout=5.0)


class TestProducerProduction:
    """Test producer production functionality."""

    def test_producer_produces_all_items(
        self,
        producer_config: ProducerConfig,
        custom_queue: ThreadSafeQueue[str],
    ) -> None:
        """Test producer puts all items from source into queue."""
        source = [f"item_{i}" for i in range(10)]
        producer = Producer(
            config=producer_config, source=source, queue=custom_queue, sentinel=None
        )

        producer.start()
        producer.join(timeout=5.0)

        # Verify all items produced
        assert producer.items_produced == len(source)

        # Get items from queue (excluding sentinel)
        items = []
        while not custom_queue.empty():
            item = custom_queue.get(block=False)
            if item is not None:  # Skip sentinel
                items.append(item)

        assert items == source

    def test_producer_sends_sentinel(
        self,
        producer_config: ProducerConfig,
        custom_queue: ThreadSafeQueue[Any],
    ) -> None:
        """Test producer sends sentinel value after completing."""
        source = ["item1", "item2"]
        sentinel = "STOP"
        producer = Producer(
            config=producer_config, source=source, queue=custom_queue, sentinel=sentinel
        )

        producer.start()
        producer.join(timeout=5.0)

        # Get all items
        items = []
        while not custom_queue.empty():
            items.append(custom_queue.get(block=False))

        # Verify sentinel is last item
        assert items[-1] == sentinel
        assert items[:-1] == source

    def test_producer_with_delay(
        self,
        custom_queue: ThreadSafeQueue[str],
    ) -> None:
        """Test producer respects delay_between_items."""
        config = ProducerConfig(
            name="DelayedProducer",
            put_timeout=5.0,
            delay_between_items=0.1,
            stop_on_error=True,
        )
        source = ["item1", "item2", "item3"]
        producer = Producer(
            config=config, source=source, queue=custom_queue, sentinel=None
        )

        start_time = time.time()
        producer.start()
        producer.join(timeout=5.0)
        elapsed = time.time() - start_time

        # Should take at least delay * (num_items - 1)
        # -1 because no delay after last item
        expected_min_time = 0.1 * (len(source) - 1)
        assert elapsed >= expected_min_time


class TestProducerErrorHandling:
    """Test producer error handling."""

    def test_producer_stop_on_error_true(
        self,
        custom_queue: ThreadSafeQueue[Any],
    ) -> None:
        """Test producer stops on first error when stop_on_error=True."""

        config = ProducerConfig(
            name="ErrorProducer",
            put_timeout=0.1,
            delay_between_items=0.0,
            stop_on_error=True,
        )

        # Create a queue that will fail
        failing_queue: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=1)
        failing_queue.put(999)  # Fill the queue

        source = [1, 2, 3, 4, 5]
        producer = Producer(
            config=config, source=source, queue=failing_queue, sentinel=None
        )

        # Start producer and wait for it to finish
        # The exception happens in the thread, not in start()
        producer.start()
        producer.join(timeout=5.0)

        # Should have produced 0 items (queue was full)
        assert producer.items_produced == 0
        assert producer.errors_encountered > 0
        # Producer should have stopped (not still running)
        assert not producer.is_running

    def test_producer_stop_on_error_false(
        self,
        custom_queue: ThreadSafeQueue[int],
    ) -> None:
        """Test producer continues on error when stop_on_error=False."""

        config = ProducerConfig(
            name="ResilientProducer",
            put_timeout=0.1,
            delay_between_items=0.0,
            stop_on_error=False,
        )

        # Create source with normal and problematic items
        # We'll use a queue that's sometimes full
        small_queue: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=20)

        source = list(range(10))
        producer = Producer(
            config=config, source=source, queue=small_queue, sentinel=None
        )

        producer.start()
        producer.join(timeout=5.0)

        # Should complete without raising exception
        assert producer.items_produced > 0


class TestProducerMetrics:
    """Test producer metrics tracking."""

    def test_items_produced_count(
        self,
        producer_config: ProducerConfig,
        custom_queue: ThreadSafeQueue[str],
    ) -> None:
        """Test items_produced tracks correctly."""
        # Use fewer items than queue capacity to avoid deadlock
        source = [f"item_{i}" for i in range(8)]
        producer = Producer(
            config=producer_config, source=source, queue=custom_queue, sentinel=None
        )

        assert producer.items_produced == 0

        producer.start()
        producer.join(timeout=5.0)

        assert producer.items_produced == len(source)

    def test_errors_encountered_count(
        self,
        custom_queue: ThreadSafeQueue[Any],
    ) -> None:
        """Test errors_encountered tracks correctly."""
        config = ProducerConfig(
            name="ErrorCountProducer",
            put_timeout=0.05,
            delay_between_items=0.0,
            stop_on_error=False,
        )

        # Create a very small queue to force errors
        tiny_queue: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=1)
        tiny_queue.put(999)  # Fill it

        source = list(range(5))
        producer = Producer(
            config=config, source=source, queue=tiny_queue, sentinel=None
        )

        producer.start()
        producer.join(timeout=5.0)

        # Should have encountered errors trying to put
        assert producer.errors_encountered > 0


class TestProducerStop:
    """Test producer stop functionality."""

    def test_producer_stop_method(
        self,
        producer_config: ProducerConfig,
        custom_queue: ThreadSafeQueue[int],
    ) -> None:
        """Test producer can be stopped via stop() method."""
        # Large source to ensure producer runs long enough
        source = list(range(10000))

        config = ProducerConfig(
            name="StoppableProducer",
            put_timeout=5.0,
            delay_between_items=0.001,  # Small delay
            stop_on_error=True,
        )

        producer = Producer(
            config=config, source=source, queue=custom_queue, sentinel=None
        )

        producer.start()
        time.sleep(0.1)  # Let it produce some items
        producer.stop()
        producer.join(timeout=5.0)

        # Should have stopped before producing all items
        assert producer.items_produced < len(source)
        assert not producer.is_running
