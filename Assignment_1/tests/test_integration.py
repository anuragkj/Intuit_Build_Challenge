"""
Integration tests for the complete producer-consumer system.

This test module validates end-to-end functionality with various configurations:
- Single producer, single consumer
- Multiple producers, single consumer
- Single producer, multiple consumers
- Multiple producers, multiple consumers
- Large data volumes
- Both custom and stdlib queue implementations
"""

import queue
import time
from typing import Any, List

import pytest

from src.config import ConsumerConfig, CoordinatorConfig, ProducerConfig
from src.consumer import Consumer
from src.coordinator import Coordinator
from src.custom_queue import ThreadSafeQueue
from src.producer import Producer


class TestSingleProducerSingleConsumer:
    """Test basic single producer, single consumer scenarios."""

    @pytest.mark.parametrize(
        "queue_impl",
        [
            ThreadSafeQueue(maxsize=10),
            queue.Queue(maxsize=10),
        ],
        ids=["custom_queue", "stdlib_queue"],
    )
    def test_basic_workflow(
        self,
        queue_impl: Any,
        source_container: List[str],
        destination_container: List[str],
    ) -> None:
        """Test basic producer-consumer workflow with both queue types."""
        producer_config = ProducerConfig(
            name="TestProducer", put_timeout=5.0, stop_on_error=True
        )
        consumer_config = ConsumerConfig(
            name="TestConsumer", get_timeout=5.0, stop_on_error=True
        )

        producer = Producer(
            config=producer_config,
            source=source_container,
            queue=queue_impl,
            sentinel=None,
        )
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=queue_impl,
            sentinel=None,
        )

        producer.start()
        consumer.start()

        producer.join(timeout=10.0)
        consumer.join(timeout=10.0)

        # Verify all items transferred
        assert destination_container == source_container
        assert producer.items_produced == len(source_container)
        assert consumer.items_consumed == len(source_container)

    @pytest.mark.parametrize(
        "queue_impl",
        [
            ThreadSafeQueue(maxsize=10),
            queue.Queue(maxsize=10),
        ],
        ids=["custom_queue", "stdlib_queue"],
    )
    def test_with_coordinator(
        self,
        queue_impl: Any,
        source_container: List[str],
        destination_container: List[str],
    ) -> None:
        """Test using Coordinator to manage threads."""
        producer_config = ProducerConfig(name="CoordProducer", put_timeout=5.0)
        consumer_config = ConsumerConfig(name="CoordConsumer", get_timeout=5.0)
        coordinator_config = CoordinatorConfig(
            name="TestCoordinator", join_timeout=10.0
        )

        producer = Producer(
            config=producer_config,
            source=source_container,
            queue=queue_impl,
            sentinel=None,
        )
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=queue_impl,
            sentinel=None,
        )

        coordinator = Coordinator(
            config=coordinator_config,
            producers=[producer],
            consumers=[consumer],
        )

        metrics = coordinator.run()

        # Verify results
        assert destination_container == source_container
        assert metrics.items_produced == len(source_container)
        assert metrics.items_consumed == len(source_container)
        assert metrics.execution_duration > 0


class TestLargeDataVolume:
    """Test with large data volumes."""

    @pytest.mark.parametrize(
        "queue_impl",
        [
            ThreadSafeQueue(maxsize=50),
            queue.Queue(maxsize=50),
        ],
        ids=["custom_queue", "stdlib_queue"],
    )
    def test_large_volume_transfer(
        self,
        queue_impl: Any,
        large_source_container: List[str],
        destination_container: List[str],
    ) -> None:
        """Test transferring 1000 items."""
        producer_config = ProducerConfig(name="LargeProducer", put_timeout=10.0)
        consumer_config = ConsumerConfig(name="LargeConsumer", get_timeout=10.0)
        coordinator_config = CoordinatorConfig(
            name="LargeCoordinator", join_timeout=30.0
        )

        producer = Producer(
            config=producer_config,
            source=large_source_container,
            queue=queue_impl,
            sentinel=None,
        )
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=queue_impl,
            sentinel=None,
        )

        coordinator = Coordinator(
            config=coordinator_config,
            producers=[producer],
            consumers=[consumer],
        )

        metrics = coordinator.run()

        # Verify no data loss
        assert len(destination_container) == len(large_source_container)
        assert destination_container == large_source_container
        assert metrics.items_produced == 1000
        assert metrics.items_consumed == 1000


class TestMultipleProducers:
    """Test multiple producers with single consumer."""

    @pytest.mark.parametrize(
        "queue_impl",
        [
            ThreadSafeQueue(maxsize=20),
            queue.Queue(maxsize=20),
        ],
        ids=["custom_queue", "stdlib_queue"],
    )
    def test_multiple_producers_single_consumer(
        self,
        queue_impl: Any,
        destination_container: List[str],
    ) -> None:
        """Test multiple producers feeding single consumer."""
        num_producers = 3
        items_per_producer = 20

        producers = []
        for i in range(num_producers):
            config = ProducerConfig(name=f"Producer_{i}", put_timeout=10.0)
            source = [f"p{i}_item_{j}" for j in range(items_per_producer)]
            producer = Producer(
                config=config, source=source, queue=queue_impl, sentinel=None
            )
            producers.append(producer)

        # Note: Need to send multiple sentinels (one per producer)
        # Create one consumer per producer for proper sentinel handling

        coordinator_config = CoordinatorConfig(name="MultiProdCoord", join_timeout=30.0)

        # Create one consumer per producer for proper sentinel handling
        consumers = []
        destinations: List[List[str]] = [[] for _ in range(num_producers)]
        for i in range(num_producers):
            config = ConsumerConfig(name=f"Consumer_{i}", get_timeout=10.0)
            consumer = Consumer(
                config=config,
                destination=destinations[i],
                queue=queue_impl,
                sentinel=None,
            )
            consumers.append(consumer)

        coordinator = Coordinator(
            config=coordinator_config,
            producers=producers,
            consumers=consumers,
        )

        # Let coordinator handle starting threads
        metrics = coordinator.run()

        # Verify all items consumed
        total_items = sum(len(dest) for dest in destinations)
        expected_total = num_producers * items_per_producer

        assert total_items == expected_total
        assert metrics.items_produced == expected_total
        assert metrics.items_consumed == expected_total


class TestMultipleConsumers:
    """Test single producer with multiple consumers."""

    @pytest.mark.parametrize(
        "queue_impl",
        [
            ThreadSafeQueue(maxsize=20),
            queue.Queue(maxsize=20),
        ],
        ids=["custom_queue", "stdlib_queue"],
    )
    def test_single_producer_multiple_consumers(
        self,
        queue_impl: Any,
        source_container: List[str],
    ) -> None:
        """Test multiple producers feeding multiple consumers (balanced)."""
        # Use equal number of producers and consumers for proper sentinel handling
        num_threads = 2

        # Create multiple producers with different data
        producers = []
        for i in range(num_threads):
            config = ProducerConfig(name=f"Producer_{i}", put_timeout=10.0)
            # Split source data among producers
            items_per_producer = len(source_container) // num_threads
            start_idx = i * items_per_producer
            end_idx = (
                start_idx + items_per_producer
                if i < num_threads - 1
                else len(source_container)
            )
            source = source_container[start_idx:end_idx]
            producer = Producer(
                config=config,
                source=source,
                queue=queue_impl,
                sentinel=None,
            )
            producers.append(producer)

        # Create equal number of consumers
        consumers = []
        destinations: List[List[str]] = [[] for _ in range(num_threads)]
        for i in range(num_threads):
            config = ConsumerConfig(name=f"Consumer_{i}", get_timeout=10.0)
            consumer = Consumer(
                config=config,
                destination=destinations[i],
                queue=queue_impl,
                sentinel=None,
            )
            consumers.append(consumer)

        coordinator_config = CoordinatorConfig(name="MultiConsCoord", join_timeout=30.0)
        coordinator = Coordinator(
            config=coordinator_config,
            producers=producers,
            consumers=consumers,
        )

        metrics = coordinator.run()

        # Verify all items consumed (distributed among consumers)
        total_items = sum(len(dest) for dest in destinations)
        assert total_items == len(source_container)

        # Verify no duplicates (combine all destinations)
        all_consumed = []
        for dest in destinations:
            all_consumed.extend(dest)

        assert sorted(all_consumed) == sorted(source_container)


class TestOrderPreservation:
    """Test FIFO order preservation."""

    @pytest.mark.parametrize(
        "queue_impl",
        [
            ThreadSafeQueue(maxsize=10),
            queue.Queue(maxsize=10),
        ],
        ids=["custom_queue", "stdlib_queue"],
    )
    def test_fifo_ordering(
        self,
        queue_impl: Any,
        destination_container: List[int],
    ) -> None:
        """Test that items are consumed in FIFO order."""
        source = list(range(100))

        producer_config = ProducerConfig(name="OrderProducer", put_timeout=10.0)
        consumer_config = ConsumerConfig(name="OrderConsumer", get_timeout=10.0)
        coordinator_config = CoordinatorConfig(name="OrderCoord", join_timeout=30.0)

        producer = Producer(
            config=producer_config,
            source=source,
            queue=queue_impl,
            sentinel=None,
        )
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=queue_impl,
            sentinel=None,
        )

        coordinator = Coordinator(
            config=coordinator_config,
            producers=[producer],
            consumers=[consumer],
        )

        coordinator.run()

        # Verify FIFO order maintained
        assert destination_container == source


class TestGracefulShutdown:
    """Test graceful shutdown scenarios."""

    def test_coordinator_shutdown(
        self,
        custom_queue: ThreadSafeQueue[str],
        source_container: List[str],
        destination_container: List[str],
    ) -> None:
        """Test coordinator ensures clean shutdown."""
        producer_config = ProducerConfig(name="ShutdownProducer", put_timeout=5.0)
        consumer_config = ConsumerConfig(name="ShutdownConsumer", get_timeout=5.0)
        coordinator_config = CoordinatorConfig(name="ShutdownCoord", join_timeout=10.0)

        producer = Producer(
            config=producer_config,
            source=source_container,
            queue=custom_queue,
            sentinel=None,
        )
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        coordinator = Coordinator(
            config=coordinator_config,
            producers=[producer],
            consumers=[consumer],
        )

        metrics = coordinator.run()

        # Verify clean shutdown
        assert not producer.is_running
        assert not consumer.is_running
        assert metrics.items_produced == len(source_container)
        assert metrics.items_consumed == len(source_container)


class TestEdgeCases:
    """Test edge cases in integration scenarios."""

    def test_empty_source(
        self,
        custom_queue: ThreadSafeQueue[str],
        destination_container: List[str],
    ) -> None:
        """Test with empty source container."""
        empty_source: List[str] = []

        producer_config = ProducerConfig(name="EmptyProducer", put_timeout=5.0)
        consumer_config = ConsumerConfig(name="EmptyConsumer", get_timeout=1.0)
        coordinator_config = CoordinatorConfig(name="EmptyCoord", join_timeout=10.0)

        producer = Producer(
            config=producer_config,
            source=empty_source,
            queue=custom_queue,
            sentinel=None,
        )
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=custom_queue,
            sentinel=None,
        )

        coordinator = Coordinator(
            config=coordinator_config,
            producers=[producer],
            consumers=[consumer],
        )

        metrics = coordinator.run()

        assert metrics.items_produced == 0
        assert metrics.items_consumed == 0
        assert len(destination_container) == 0

    def test_tiny_queue_capacity(
        self,
        source_container: List[str],
        destination_container: List[str],
    ) -> None:
        """Test with queue capacity of 1."""
        tiny_queue: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=1)

        producer_config = ProducerConfig(name="TinyProducer", put_timeout=10.0)
        consumer_config = ConsumerConfig(name="TinyConsumer", get_timeout=10.0)
        coordinator_config = CoordinatorConfig(name="TinyCoord", join_timeout=30.0)

        producer = Producer(
            config=producer_config,
            source=source_container,
            queue=tiny_queue,
            sentinel=None,
        )
        consumer = Consumer(
            config=consumer_config,
            destination=destination_container,
            queue=tiny_queue,
            sentinel=None,
        )

        coordinator = Coordinator(
            config=coordinator_config,
            producers=[producer],
            consumers=[consumer],
        )

        metrics = coordinator.run()

        # Should still work correctly despite tiny capacity
        assert destination_container == source_container
        assert metrics.items_produced == len(source_container)
        assert metrics.items_consumed == len(source_container)
