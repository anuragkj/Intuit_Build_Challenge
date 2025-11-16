"""
Unit tests for the Consumer class.

This test module validates Consumer behavior including:
- Basic consumption functionality
- Sentinel detection and shutdown
- Timeout handling
- Error handling
- Metrics tracking
"""

import time
from typing import Any, List

import pytest

from src.config import ConsumerConfig
from src.consumer import Consumer
from src.custom_queue import ThreadSafeQueue
from src.exceptions import ConsumerError


class TestConsumerBasics:
    """Test basic consumer functionality."""

    def test_consumer_initialization(
        self,
        consumer_config: ConsumerConfig,
        custom_queue: ThreadSafeQueue[Any],
        destination_container: List[Any],
    ) -> None:
        """Test consumer initializes correctly."""
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        assert consumer.config == consumer_config
        assert consumer.destination == destination_container
        assert consumer.queue == custom_queue
        assert consumer.sentinel is None
        assert consumer.items_consumed == 0
        assert not consumer.is_running

    def test_consumer_starts_and_joins(
        self,
        consumer_config: ConsumerConfig,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test consumer starts and completes when sentinel received."""
        # Pre-fill queue with items and sentinel
        custom_queue.put("item1")
        custom_queue.put("item2")
        custom_queue.put(None)  # Sentinel

        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        consumer.start()
        # Note: Not checking is_running immediately due to race condition
        # Consumer may finish too quickly if items are already in queue

        completed = consumer.join(timeout=5.0)
        assert completed
        assert not consumer.is_running
        assert destination_container == ["item1", "item2"]

    def test_consumer_cannot_start_twice(
        self,
        consumer_config: ConsumerConfig,
        custom_queue: ThreadSafeQueue[Any],
        destination_container: List[Any],
    ) -> None:
        """Test that starting consumer twice raises error."""
        # Put several items to keep consumer running longer
        # Don't put sentinel yet to ensure consumer stays active
        for i in range(5):
            custom_queue.put(f"item_{i}")

        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        consumer.start()
        # Small delay to ensure consumer thread has started
        time.sleep(0.1)

        with pytest.raises(ConsumerError):
            consumer.start()

        # Now send sentinel to allow consumer to finish
        custom_queue.put(None)
        consumer.join(timeout=5.0)


class TestConsumerConsumption:
    """Test consumer consumption functionality."""

    def test_consumer_consumes_all_items(
        self,
        consumer_config: ConsumerConfig,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test consumer gets all items from queue until sentinel."""
        # Use fewer items than queue capacity to avoid deadlock
        # Queue capacity is 10, so use 8 items + sentinel = 9 total
        items = [f"item_{i}" for i in range(8)]

        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        # Start consumer first to avoid deadlock
        consumer.start()

        # Then fill queue
        for item in items:
            custom_queue.put(item)
        custom_queue.put(None)  # Sentinel

        consumer.join(timeout=5.0)

        # Verify all items consumed
        assert consumer.items_consumed == len(items)
        assert destination_container == items

    def test_consumer_stops_on_sentinel(
        self,
        consumer_config: ConsumerConfig,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test consumer stops when sentinel is received."""
        sentinel = "STOP_SIGNAL"

        custom_queue.put("item1")
        custom_queue.put("item2")
        custom_queue.put(sentinel)  # Sentinel
        custom_queue.put("item3")  # Should not be consumed

        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=sentinel,
        )

        consumer.start()
        consumer.join(timeout=5.0)

        # Verify stopped at sentinel
        assert destination_container == ["item1", "item2"]
        assert consumer.items_consumed == 2

    def test_consumer_with_delay(
        self,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test consumer respects delay_between_items."""
        config = ConsumerConfig(
            name="DelayedConsumer",
            get_timeout=5.0,
            delay_between_items=0.1,
            stop_on_error=True,
        )

        # Pre-fill queue
        items = ["item1", "item2", "item3"]
        for item in items:
            custom_queue.put(item)
        custom_queue.put(None)  # Sentinel

        consumer = Consumer(
            config=config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        start_time = time.time()
        consumer.start()
        consumer.join(timeout=5.0)
        elapsed = time.time() - start_time

        # Should take at least delay * num_items
        expected_min_time = 0.1 * len(items)
        assert elapsed >= expected_min_time


class TestConsumerErrorHandling:
    """Test consumer error handling."""

    def test_consumer_timeout_continues(
        self,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test consumer continues after get timeout."""
        config = ConsumerConfig(
            name="TimeoutConsumer",
            get_timeout=0.2,
            delay_between_items=0.0,
            stop_on_error=True,
        )

        consumer = Consumer(
            config=config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        consumer.start()

        # Let consumer timeout a few times
        time.sleep(0.7)

        # Now send items and sentinel
        custom_queue.put("item1")
        custom_queue.put(None)

        consumer.join(timeout=5.0)

        # Should have consumed the item despite earlier timeouts
        assert destination_container == ["item1"]

    def test_consumer_stop_on_error_true(
        self,
        custom_queue: ThreadSafeQueue[Any],
    ) -> None:
        """Test consumer stops on first error when stop_on_error=True."""
        config = ConsumerConfig(
            name="ErrorConsumer",
            get_timeout=5.0,
            delay_between_items=0.0,
            stop_on_error=True,
        )

        # Create a destination that will fail
        class FailingDestination:
            def append(self, item: Any) -> None:
                raise RuntimeError("Destination error")

        failing_dest = FailingDestination()

        custom_queue.put("item1")
        custom_queue.put(None)  # Sentinel

        consumer = Consumer(
            config=config,
            destination=failing_dest,  # type: ignore
            queue=custom_queue,
            sentinel=None,
        )

        # Start consumer and wait for it to finish
        # The exception happens in the thread, not in start()
        consumer.start()
        consumer.join(timeout=5.0)

        # Verify the consumer encountered errors
        assert consumer.errors_encountered > 0
        # Consumer should have stopped (not still running)
        assert not consumer.is_running

    def test_consumer_stop_on_error_false(
        self,
        custom_queue: ThreadSafeQueue[Any],
    ) -> None:
        """Test consumer continues on error when stop_on_error=False."""
        config = ConsumerConfig(
            name="ResilientConsumer",
            get_timeout=5.0,
            delay_between_items=0.0,
            stop_on_error=False,
        )

        # Destination that tracks successful appends
        successful_items: List[Any] = []

        class SometimesFailingDestination:
            def __init__(self) -> None:
                self.call_count = 0

            def append(self, item: Any) -> None:
                self.call_count += 1
                if self.call_count == 2:  # Fail on second item
                    raise RuntimeError("Simulated error")
                successful_items.append(item)

        dest = SometimesFailingDestination()

        custom_queue.put("item1")
        custom_queue.put("item2")  # This will fail
        custom_queue.put("item3")
        custom_queue.put(None)  # Sentinel

        consumer = Consumer(
            config=config,
            destination=dest,  # type: ignore
            queue=custom_queue,
            sentinel=None,
        )

        consumer.start()
        consumer.join(timeout=5.0)

        # Should have continued after error
        assert "item1" in successful_items
        assert "item3" in successful_items
        assert consumer.errors_encountered > 0


class TestConsumerMetrics:
    """Test consumer metrics tracking."""

    def test_items_consumed_count(
        self,
        consumer_config: ConsumerConfig,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test items_consumed tracks correctly."""
        # Use fewer items than queue capacity to avoid deadlock
        items = [f"item_{i}" for i in range(8)]

        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        assert consumer.items_consumed == 0

        # Start consumer first
        consumer.start()

        # Then fill queue
        for item in items:
            custom_queue.put(item)
        custom_queue.put(None)  # Sentinel

        consumer.join(timeout=5.0)

        assert consumer.items_consumed == len(items)


class TestConsumerStop:
    """Test consumer stop functionality."""

    def test_consumer_stop_method(
        self,
        consumer_config: ConsumerConfig,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test consumer can be stopped via stop() method."""
        # Add items without sentinel to keep consumer running
        for i in range(10):
            custom_queue.put(f"item_{i}")

        config = ConsumerConfig(
            name="StoppableConsumer",
            get_timeout=1.0,
            delay_between_items=0.1,
            stop_on_error=True,
        )

        consumer = Consumer(
            config=config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        consumer.start()
        time.sleep(0.3)  # Let it consume some items
        consumer.stop()
        consumer.join(timeout=5.0)

        # Should have stopped
        assert not consumer.is_running
        assert consumer.items_consumed < 10  # Didn't consume all
