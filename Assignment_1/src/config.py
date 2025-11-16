"""
Configuration classes for the producer-consumer system.

This module defines dataclasses for configuring producers, consumers, and
the overall coordinator. Using dataclasses provides type safety, immutability
options, and clear configuration interfaces.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class ProducerConfig:
    """
    Configuration for a Producer instance.

    This immutable configuration object defines how a producer should behave,
    including timing parameters and error handling options.

    Attributes:
        name: Identifier for the producer (used in logging)
        put_timeout: Maximum seconds to wait when putting to a full queue
                    (None = wait indefinitely)
        delay_between_items: Seconds to sleep between producing items
                            (simulates processing time, 0 = no delay)
        stop_on_error: If True, stop producing on first error; if False,
                      log error and continue with next item
    """

    name: str
    put_timeout: Optional[float] = None
    delay_between_items: float = 0.0
    stop_on_error: bool = True


@dataclass(frozen=True)
class ConsumerConfig:
    """
    Configuration for a Consumer instance.

    This immutable configuration object defines how a consumer should behave,
    including timing parameters and error handling options.

    Attributes:
        name: Identifier for the consumer (used in logging)
        get_timeout: Maximum seconds to wait when getting from an empty queue
                    (None = wait indefinitely)
        delay_between_items: Seconds to sleep between consuming items
                            (simulates processing time, 0 = no delay)
        stop_on_error: If True, stop consuming on first error; if False,
                      log error and continue with next item
    """

    name: str
    get_timeout: Optional[float] = None
    delay_between_items: float = 0.0
    stop_on_error: bool = True


@dataclass
class CoordinatorConfig:
    """
    Configuration for the Coordinator that manages the producer-consumer system.

    This configuration controls the overall behavior of the system including
    thread management and shutdown procedures.

    Attributes:
        name: Identifier for the coordinator (used in logging)
        join_timeout: Maximum seconds to wait for threads to join during shutdown
                     (None = wait indefinitely)
        sentinel_value: Special value sent to signal consumer shutdown
                       (default: None)
        shutdown_grace_period: Seconds to wait after sending sentinel before
                              forcing thread termination (default: 5.0)
    """

    name: str = "Coordinator"
    join_timeout: Optional[float] = 30.0
    sentinel_value: Optional[object] = None
    shutdown_grace_period: float = 5.0


@dataclass
class SystemMetrics:
    """
    Metrics collected during system execution.

    This mutable dataclass accumulates statistics about the producer-consumer
    system performance and behavior for analysis and comparison.

    Attributes:
        items_produced: Total number of items produced
        items_consumed: Total number of items consumed
        producer_errors: Number of errors encountered by producers
        consumer_errors: Number of errors encountered by consumers
        start_time: System start timestamp (seconds since epoch)
        end_time: System end timestamp (seconds since epoch)
        execution_duration: Total execution time in seconds
    """

    items_produced: int = 0
    items_consumed: int = 0
    producer_errors: int = 0
    consumer_errors: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    execution_duration: float = 0.0

    def calculate_duration(self) -> None:
        """Calculate and update execution duration based on start/end times."""
        if self.start_time and self.end_time:
            self.execution_duration = self.end_time - self.start_time

    def __str__(self) -> str:
        """Return a formatted string representation of metrics."""
        return (
            f"SystemMetrics("
            f"produced={self.items_produced}, "
            f"consumed={self.items_consumed}, "
            f"producer_errors={self.producer_errors}, "
            f"consumer_errors={self.consumer_errors}, "
            f"duration={self.execution_duration:.3f}s)"
        )

