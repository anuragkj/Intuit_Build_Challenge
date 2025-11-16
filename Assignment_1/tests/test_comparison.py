"""
Comparison tests validating custom queue vs stdlib queue.

This test module runs identical workloads with both the custom ThreadSafeQueue
and Python's standard library queue.Queue to validate correctness of the
custom implementation.
"""

import queue
from typing import Any, List

import pytest

from src.config import ConsumerConfig, CoordinatorConfig, ProducerConfig
from src.consumer import Consumer
from src.coordinator import Coordinator
from src.custom_queue import ThreadSafeQueue
from src.producer import Producer


class TestCustomVsStdlib:
    """Compare custom queue implementation against stdlib."""

    def test_same_results_simple_workload(
        self,
        source_container: List[str],
    ) -> None:
        """Test that both queues produce identical results for simple workload."""
        # Run with custom queue
        custom_queue_impl: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=10)
        custom_destination: List[str] = []

        custom_producer = Producer(
            config=ProducerConfig(name="CustomProducer", put_timeout=5.0),
            source=source_container.copy(),
            queue=custom_queue_impl,
            sentinel=None,
        )
        custom_consumer = Consumer(
            config=ConsumerConfig(name="CustomConsumer", get_timeout=5.0),
            destination=custom_destination,
            queue=custom_queue_impl,
            sentinel=None,
        )

        custom_coord = Coordinator(
            config=CoordinatorConfig(name="CustomCoord", join_timeout=10.0),
            producers=[custom_producer],
            consumers=[custom_consumer],
        )
        custom_metrics = custom_coord.run()

        # Run with stdlib queue
        stdlib_queue_impl: queue.Queue[str] = queue.Queue(maxsize=10)
        stdlib_destination: List[str] = []

        stdlib_producer = Producer(
            config=ProducerConfig(name="StdlibProducer", put_timeout=5.0),
            source=source_container.copy(),
            queue=stdlib_queue_impl,
            sentinel=None,
        )
        stdlib_consumer = Consumer(
            config=ConsumerConfig(name="StdlibConsumer", get_timeout=5.0),
            destination=stdlib_destination,
            queue=stdlib_queue_impl,
            sentinel=None,
        )

        stdlib_coord = Coordinator(
            config=CoordinatorConfig(name="StdlibCoord", join_timeout=10.0),
            producers=[stdlib_producer],
            consumers=[stdlib_consumer],
        )
        stdlib_metrics = stdlib_coord.run()

        # Compare results
        assert custom_destination == stdlib_destination
        assert custom_destination == source_container
        assert custom_metrics.items_produced == stdlib_metrics.items_produced
        assert custom_metrics.items_consumed == stdlib_metrics.items_consumed

    def test_same_results_large_workload(
        self,
        large_source_container: List[str],
    ) -> None:
        """Test that both queues handle large workloads identically."""
        # Run with custom queue
        custom_queue_impl: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=50)
        custom_destination: List[str] = []

        custom_producer = Producer(
            config=ProducerConfig(name="LargeCustomProducer", put_timeout=10.0),
            source=large_source_container.copy(),
            queue=custom_queue_impl,
            sentinel=None,
        )
        custom_consumer = Consumer(
            config=ConsumerConfig(name="LargeCustomConsumer", get_timeout=10.0),
            destination=custom_destination,
            queue=custom_queue_impl,
            sentinel=None,
        )

        custom_coord = Coordinator(
            config=CoordinatorConfig(name="LargeCustomCoord", join_timeout=30.0),
            producers=[custom_producer],
            consumers=[custom_consumer],
        )
        custom_metrics = custom_coord.run()

        # Run with stdlib queue
        stdlib_queue_impl: queue.Queue[str] = queue.Queue(maxsize=50)
        stdlib_destination: List[str] = []

        stdlib_producer = Producer(
            config=ProducerConfig(name="LargeStdlibProducer", put_timeout=10.0),
            source=large_source_container.copy(),
            queue=stdlib_queue_impl,
            sentinel=None,
        )
        stdlib_consumer = Consumer(
            config=ConsumerConfig(name="LargeStdlibConsumer", get_timeout=10.0),
            destination=stdlib_destination,
            queue=stdlib_queue_impl,
            sentinel=None,
        )

        stdlib_coord = Coordinator(
            config=CoordinatorConfig(name="LargeStdlibCoord", join_timeout=30.0),
            producers=[stdlib_producer],
            consumers=[stdlib_consumer],
        )
        stdlib_metrics = stdlib_coord.run()

        # Compare results
        assert len(custom_destination) == len(stdlib_destination)
        assert len(custom_destination) == len(large_source_container)
        assert custom_destination == large_source_container
        assert stdlib_destination == large_source_container
        assert custom_metrics.items_produced == stdlib_metrics.items_produced
        assert custom_metrics.items_consumed == stdlib_metrics.items_consumed
        assert custom_metrics.items_produced == 1000

    def test_both_queues_maintain_fifo_order(self) -> None:
        """Test that both queues maintain FIFO ordering."""
        source = list(range(100))

        # Custom queue
        custom_q: ThreadSafeQueue[int] = ThreadSafeQueue(maxsize=10)
        custom_dest: List[int] = []

        custom_prod = Producer(
            config=ProducerConfig(name="CustomOrderProd", put_timeout=10.0),
            source=source.copy(),
            queue=custom_q,
            sentinel=None,
        )
        custom_cons = Consumer(
            config=ConsumerConfig(name="CustomOrderCons", get_timeout=10.0),
            destination=custom_dest,
            queue=custom_q,
            sentinel=None,
        )

        custom_coord = Coordinator(
            config=CoordinatorConfig(name="CustomOrderCoord", join_timeout=30.0),
            producers=[custom_prod],
            consumers=[custom_cons],
        )
        custom_coord.run()

        # Stdlib queue
        stdlib_q: queue.Queue[int] = queue.Queue(maxsize=10)
        stdlib_dest: List[int] = []

        stdlib_prod = Producer(
            config=ProducerConfig(name="StdlibOrderProd", put_timeout=10.0),
            source=source.copy(),
            queue=stdlib_q,
            sentinel=None,
        )
        stdlib_cons = Consumer(
            config=ConsumerConfig(name="StdlibOrderCons", get_timeout=10.0),
            destination=stdlib_dest,
            queue=stdlib_q,
            sentinel=None,
        )

        stdlib_coord = Coordinator(
            config=CoordinatorConfig(name="StdlibOrderCoord", join_timeout=30.0),
            producers=[stdlib_prod],
            consumers=[stdlib_cons],
        )
        stdlib_coord.run()

        # Both should maintain FIFO order
        assert custom_dest == source
        assert stdlib_dest == source
        assert custom_dest == stdlib_dest

    def test_both_queues_handle_blocking_correctly(self) -> None:
        """Test that both queues handle blocking behavior similarly."""
        source = [f"item_{i}" for i in range(50)]

        # Use small queue capacity to force blocking
        # Custom queue
        custom_q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=5)
        custom_dest: List[str] = []

        custom_prod = Producer(
            config=ProducerConfig(
                name="CustomBlockProd",
                put_timeout=10.0,
                delay_between_items=0.01,
            ),
            source=source.copy(),
            queue=custom_q,
            sentinel=None,
        )
        custom_cons = Consumer(
            config=ConsumerConfig(
                name="CustomBlockCons",
                get_timeout=10.0,
                delay_between_items=0.01,
            ),
            destination=custom_dest,
            queue=custom_q,
            sentinel=None,
        )

        custom_coord = Coordinator(
            config=CoordinatorConfig(name="CustomBlockCoord", join_timeout=30.0),
            producers=[custom_prod],
            consumers=[custom_cons],
        )
        custom_metrics = custom_coord.run()

        # Stdlib queue
        stdlib_q: queue.Queue[str] = queue.Queue(maxsize=5)
        stdlib_dest: List[str] = []

        stdlib_prod = Producer(
            config=ProducerConfig(
                name="StdlibBlockProd",
                put_timeout=10.0,
                delay_between_items=0.01,
            ),
            source=source.copy(),
            queue=stdlib_q,
            sentinel=None,
        )
        stdlib_cons = Consumer(
            config=ConsumerConfig(
                name="StdlibBlockCons",
                get_timeout=10.0,
                delay_between_items=0.01,
            ),
            destination=stdlib_dest,
            queue=stdlib_q,
            sentinel=None,
        )

        stdlib_coord = Coordinator(
            config=CoordinatorConfig(name="StdlibBlockCoord", join_timeout=30.0),
            producers=[stdlib_prod],
            consumers=[stdlib_cons],
        )
        stdlib_metrics = stdlib_coord.run()

        # Both should complete successfully with correct results
        assert custom_dest == source
        assert stdlib_dest == source
        assert custom_metrics.items_produced == len(source)
        assert stdlib_metrics.items_produced == len(source)
        assert custom_metrics.items_consumed == len(source)
        assert stdlib_metrics.items_consumed == len(source)


class TestPerformanceComparison:
    """Compare performance characteristics (informational only)."""

    def test_performance_comparison_informational(
        self,
        large_source_container: List[str],
    ) -> None:
        """
        Compare execution times between custom and stdlib queues.

        Note: This is informational and may vary based on system load.
        We don't assert on timing, just log the comparison.
        """
        # Custom queue
        custom_q: ThreadSafeQueue[str] = ThreadSafeQueue(maxsize=50)
        custom_dest: List[str] = []

        custom_prod = Producer(
            config=ProducerConfig(name="PerfCustomProd", put_timeout=10.0),
            source=large_source_container.copy(),
            queue=custom_q,
            sentinel=None,
        )
        custom_cons = Consumer(
            config=ConsumerConfig(name="PerfCustomCons", get_timeout=10.0),
            destination=custom_dest,
            queue=custom_q,
            sentinel=None,
        )

        custom_coord = Coordinator(
            config=CoordinatorConfig(name="PerfCustomCoord", join_timeout=30.0),
            producers=[custom_prod],
            consumers=[custom_cons],
        )
        custom_metrics = custom_coord.run()

        # Stdlib queue
        stdlib_q: queue.Queue[str] = queue.Queue(maxsize=50)
        stdlib_dest: List[str] = []

        stdlib_prod = Producer(
            config=ProducerConfig(name="PerfStdlibProd", put_timeout=10.0),
            source=large_source_container.copy(),
            queue=stdlib_q,
            sentinel=None,
        )
        stdlib_cons = Consumer(
            config=ConsumerConfig(name="PerfStdlibCons", get_timeout=10.0),
            destination=stdlib_dest,
            queue=stdlib_q,
            sentinel=None,
        )

        stdlib_coord = Coordinator(
            config=CoordinatorConfig(name="PerfStdlibCoord", join_timeout=30.0),
            producers=[stdlib_prod],
            consumers=[stdlib_cons],
        )
        stdlib_metrics = stdlib_coord.run()

        # Verify correctness first
        assert custom_dest == stdlib_dest
        assert len(custom_dest) == 1000

        # Print performance comparison (informational)
        print(f"\nPerformance Comparison (1000 items):")
        print(f"  Custom Queue:  {custom_metrics.execution_duration:.3f}s")
        print(f"  Stdlib Queue:  {stdlib_metrics.execution_duration:.3f}s")

        # Both should complete in reasonable time (not hanging)
        assert custom_metrics.execution_duration < 60.0
        assert stdlib_metrics.execution_duration < 60.0
