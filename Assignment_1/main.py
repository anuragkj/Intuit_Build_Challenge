"""
Producer-Consumer Pattern Demonstration Script.

This script demonstrates the producer-consumer pattern implementation with both:
1. Custom ThreadSafeQueue implementation (using Lock and Condition)
2. Standard library queue.Queue implementation

The demonstration showcases:
- Thread synchronization using low-level primitives
- Concurrent programming with multiple producers/consumers
- Blocking queue operations
- Wait/notify mechanism (condition variables)
- Graceful shutdown using sentinel pattern
- Metrics collection and comparison
"""

import logging
import queue
import sys
from typing import Any, List

from src.config import ConsumerConfig, CoordinatorConfig, ProducerConfig
from src.consumer import Consumer
from src.coordinator import Coordinator
from src.custom_queue import ThreadSafeQueue
from src.producer import Producer


def setup_logging() -> None:
    """
    Configure logging for the demonstration.

    Sets up console logging with timestamp, level, thread name, and message.
    This provides visibility into thread operations and synchronization.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)-8s [%(threadName)-15s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def print_section_header(title: str) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_subsection_header(title: str) -> None:
    """Print a formatted subsection header."""
    print("\n" + "-" * 80)
    print(f" {title}")
    print("-" * 80)


def run_custom_queue_demo() -> None:
    """
    Demonstrate the custom ThreadSafeQueue implementation.

    This showcases:
    - Custom queue built with threading.Lock and threading.Condition
    - Bounded blocking queue with capacity management
    - Wait/notify pattern for producer-consumer coordination
    - Thread-safe operations with proper synchronization
    """
    print_subsection_header("Custom ThreadSafeQueue Implementation")

    # Create source data
    source_data = [f"item_{i}" for i in range(100)]
    destination: List[str] = []

    # Create custom queue with bounded capacity
    custom_queue: ThreadSafeQueue[Any] = ThreadSafeQueue(maxsize=10)

    # Configure producer
    producer_config = ProducerConfig(
        name="CustomProducer-1",
        put_timeout=30.0,
        delay_between_items=0.01,  # Small delay to simulate work
        stop_on_error=True,
    )

    # Configure consumer
    consumer_config = ConsumerConfig(
        name="CustomConsumer-1",
        get_timeout=30.0,
        delay_between_items=0.01,  # Small delay to simulate work
        stop_on_error=True,
    )

    # Configure coordinator
    coordinator_config = CoordinatorConfig(
        name="CustomQueueCoordinator",
        join_timeout=60.0,
        sentinel_value=None,
        shutdown_grace_period=5.0,
    )

    # Create producer and consumer
    producer = Producer(
        config=producer_config,
        source=source_data,
        queue=custom_queue,
        sentinel=None,
    )

    consumer = Consumer(
        config=consumer_config,
        destination=destination,
        queue=custom_queue,
        sentinel=None,
    )

    # Create coordinator
    coordinator = Coordinator(
        config=coordinator_config,
        producers=[producer],
        consumers=[consumer],
    )

    print("\nStarting custom queue demonstration...")
    print(f"  Source items: {len(source_data)}")
    print(f"  Queue capacity: {custom_queue._maxsize}")
    print(f"  Queue type: {type(custom_queue).__name__}")

    # Run the system
    metrics = coordinator.run()

    # Display results
    print("\nCustom Queue Results:")
    print(f"  Items produced: {metrics.items_produced}")
    print(f"  Items consumed: {metrics.items_consumed}")
    print(f"  Producer errors: {metrics.producer_errors}")
    print(f"  Consumer errors: {metrics.consumer_errors}")
    print(f"  Execution time: {metrics.execution_duration:.3f}s")
    print(f"  Items lost: {metrics.items_produced - metrics.items_consumed}")
    print(f"  Order preserved: {destination == source_data}")

    # Verify correctness
    assert destination == source_data, "Data integrity check failed!"
    print("\n✓ Custom queue demonstration completed successfully!")


def run_stdlib_queue_demo() -> None:
    """
    Demonstrate the standard library queue.Queue implementation.

    This showcases:
    - Using Python's battle-tested queue.Queue
    - Same producer-consumer pattern with stdlib implementation
    - Comparison point for validating custom implementation
    - Professional judgment: using libraries when appropriate
    """
    print_subsection_header("Standard Library queue.Queue Implementation")

    # Create source data
    source_data = [f"item_{i}" for i in range(100)]
    destination: List[str] = []

    # Create stdlib queue with bounded capacity
    stdlib_queue: queue.Queue[Any] = queue.Queue(maxsize=10)

    # Configure producer
    producer_config = ProducerConfig(
        name="StdlibProducer-1",
        put_timeout=30.0,
        delay_between_items=0.01,
        stop_on_error=True,
    )

    # Configure consumer
    consumer_config = ConsumerConfig(
        name="StdlibConsumer-1",
        get_timeout=30.0,
        delay_between_items=0.01,
        stop_on_error=True,
    )

    # Configure coordinator
    coordinator_config = CoordinatorConfig(
        name="StdlibQueueCoordinator",
        join_timeout=60.0,
        sentinel_value=None,
        shutdown_grace_period=5.0,
    )

    # Create producer and consumer
    producer = Producer(
        config=producer_config,
        source=source_data,
        queue=stdlib_queue,
        sentinel=None,
    )

    consumer = Consumer(
        config=consumer_config,
        destination=destination,
        queue=stdlib_queue,
        sentinel=None,
    )

    # Create coordinator
    coordinator = Coordinator(
        config=coordinator_config,
        producers=[producer],
        consumers=[consumer],
    )

    print("\nStarting stdlib queue demonstration...")
    print(f"  Source items: {len(source_data)}")
    print(f"  Queue capacity: {stdlib_queue.maxsize}")
    print(f"  Queue type: {type(stdlib_queue).__name__}")

    # Run the system
    metrics = coordinator.run()

    # Display results
    print("\nStdlib Queue Results:")
    print(f"  Items produced: {metrics.items_produced}")
    print(f"  Items consumed: {metrics.items_consumed}")
    print(f"  Producer errors: {metrics.producer_errors}")
    print(f"  Consumer errors: {metrics.consumer_errors}")
    print(f"  Execution time: {metrics.execution_duration:.3f}s")
    print(f"  Items lost: {metrics.items_produced - metrics.items_consumed}")
    print(f"  Order preserved: {destination == source_data}")

    # Verify correctness
    assert destination == source_data, "Data integrity check failed!"
    print("\n✓ Stdlib queue demonstration completed successfully!")


def run_multiple_producers_consumers_demo() -> None:
    """
    Demonstrate multiple producers and consumers with custom queue.

    This showcases:
    - Concurrent access from multiple threads
    - Thread safety with contention
    - Coordination of multiple producers and consumers
    - Scalability of the implementation
    
    Note: With multiple producers and multiple consumers, the sentinel pattern
    requires careful handling. We use one producer per consumer to ensure
    proper shutdown signaling (each consumer gets exactly one sentinel).
    """
    print_subsection_header(
        "Multiple Producers & Consumers with Custom Queue"
    )

    num_producers = 3
    num_consumers = 3  # Match producers for proper sentinel handling
    items_per_producer = 50

    # Create custom queue
    custom_queue: ThreadSafeQueue[Any] = ThreadSafeQueue(maxsize=20)

    # Configure coordinator
    coordinator_config = CoordinatorConfig(
        name="MultiThreadCoordinator",
        join_timeout=60.0,
        sentinel_value=None,
        shutdown_grace_period=5.0,
    )

    # Create multiple producers
    producers = []
    for i in range(num_producers):
        source_data = [
            f"producer{i}_item_{j}" for j in range(items_per_producer)
        ]
        config = ProducerConfig(
            name=f"Producer-{i+1}",
            put_timeout=30.0,
            delay_between_items=0.005,
            stop_on_error=True,
        )
        producer = Producer(
            config=config,
            source=source_data,
            queue=custom_queue,
            sentinel=None,
        )
        producers.append(producer)

    # Create multiple consumers (same count as producers for sentinel pattern)
    consumers = []
    destinations: List[List[Any]] = []
    for i in range(num_consumers):
        destination: List[Any] = []
        destinations.append(destination)
        config = ConsumerConfig(
            name=f"Consumer-{i+1}",
            get_timeout=30.0,
            delay_between_items=0.005,
            stop_on_error=True,
        )
        consumer = Consumer(
            config=config,
            destination=destination,
            queue=custom_queue,
            sentinel=None,
        )
        consumers.append(consumer)

    # Create coordinator
    coordinator = Coordinator(
        config=coordinator_config,
        producers=producers,
        consumers=consumers,
    )

    print(f"\nStarting multi-threaded demonstration...")
    print(f"  Producers: {num_producers}")
    print(f"  Consumers: {num_consumers}")
    print(f"  Items per producer: {items_per_producer}")
    print(f"  Total items: {num_producers * items_per_producer}")
    print(f"  Queue capacity: {custom_queue._maxsize}")
    print(f"  Note: Producers = Consumers for proper sentinel handling")

    # Run the system
    metrics = coordinator.run()

    # Aggregate results
    total_consumed = sum(len(dest) for dest in destinations)

    # Display results
    print("\nMulti-Thread Results:")
    print(f"  Items produced: {metrics.items_produced}")
    print(f"  Items consumed: {metrics.items_consumed}")
    print(f"  Producer errors: {metrics.producer_errors}")
    print(f"  Consumer errors: {metrics.consumer_errors}")
    print(f"  Execution time: {metrics.execution_duration:.3f}s")
    print(f"  Items lost: {metrics.items_produced - metrics.items_consumed}")

    # Per-consumer breakdown
    print("\n  Per-Consumer Breakdown:")
    for i, dest in enumerate(destinations):
        print(f"    Consumer-{i+1}: {len(dest)} items")

    # Verify correctness
    assert total_consumed == num_producers * items_per_producer, (
        f"Data loss detected! Expected {num_producers * items_per_producer}, "
        f"got {total_consumed}"
    )
    print("\n✓ Multi-threaded demonstration completed successfully!")


def run_large_volume_demo() -> None:
    """
    Demonstrate handling large data volumes.

    This showcases:
    - System performance with high throughput
    - Stability under load
    - Efficient queue operations
    - No data loss at scale
    """
    print_subsection_header("Large Volume Transfer (10,000 items)")

    # Create large source data
    source_data = [f"item_{i:05d}" for i in range(10000)]
    destination: List[str] = []

    # Create custom queue
    custom_queue: ThreadSafeQueue[Any] = ThreadSafeQueue(maxsize=100)

    # Configure producer
    producer_config = ProducerConfig(
        name="HighVolumeProducer",
        put_timeout=60.0,
        delay_between_items=0.0,  # No delay for speed
        stop_on_error=True,
    )

    # Configure consumer
    consumer_config = ConsumerConfig(
        name="HighVolumeConsumer",
        get_timeout=60.0,
        delay_between_items=0.0,  # No delay for speed
        stop_on_error=True,
    )

    # Configure coordinator
    coordinator_config = CoordinatorConfig(
        name="HighVolumeCoordinator",
        join_timeout=120.0,
        sentinel_value=None,
        shutdown_grace_period=10.0,
    )

    # Create producer and consumer
    producer = Producer(
        config=producer_config,
        source=source_data,
        queue=custom_queue,
        sentinel=None,
    )

    consumer = Consumer(
        config=consumer_config,
        destination=destination,
        queue=custom_queue,
        sentinel=None,
    )

    # Create coordinator
    coordinator = Coordinator(
        config=coordinator_config,
        producers=[producer],
        consumers=[consumer],
    )

    print(f"\nStarting high-volume demonstration...")
    print(f"  Total items: {len(source_data):,}")
    print(f"  Queue capacity: {custom_queue._maxsize}")

    # Run the system
    metrics = coordinator.run()

    # Calculate throughput
    throughput = (
        metrics.items_consumed / metrics.execution_duration
        if metrics.execution_duration > 0
        else 0
    )

    # Display results
    print("\nHigh Volume Results:")
    print(f"  Items produced: {metrics.items_produced:,}")
    print(f"  Items consumed: {metrics.items_consumed:,}")
    print(f"  Execution time: {metrics.execution_duration:.3f}s")
    print(f"  Throughput: {throughput:.0f} items/second")
    print(f"  Items lost: {metrics.items_produced - metrics.items_consumed}")
    print(f"  Order preserved: {destination == source_data}")

    # Verify correctness
    assert destination == source_data, "Data integrity check failed!"
    print("\n✓ High-volume demonstration completed successfully!")


def main() -> None:
    """
    Main demonstration entry point.

    Runs a comprehensive demonstration of the producer-consumer pattern
    implementation showcasing various scenarios and both queue implementations.
    """
    setup_logging()

    print_section_header("Producer-Consumer Pattern Demonstration")
    print("\nThis demonstration showcases:")
    print("  1. Custom ThreadSafeQueue (Lock + Condition variables)")
    print("  2. Standard library queue.Queue (for comparison)")
    print("  3. Multiple producers and consumers")
    print("  4. Large volume data transfer")
    print("  5. Thread synchronization and graceful shutdown")

    try:
        # Demo 1: Custom queue
        run_custom_queue_demo()

        # Demo 2: Stdlib queue
        run_stdlib_queue_demo()

        # Demo 3: Multiple producers/consumers
        run_multiple_producers_consumers_demo()

        # Demo 4: Large volume
        run_large_volume_demo()
        
    except Exception as e:
        logging.error(f"Demonstration failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

