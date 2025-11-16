"""
Coordinator for managing the producer-consumer system.

This module implements the Coordinator class which orchestrates the lifecycle
of producer and consumer threads, manages graceful shutdown, and collects
system-wide metrics.
"""

import logging
import time
from typing import Any, List, Optional

from src.config import CoordinatorConfig, SystemMetrics
from src.consumer import Consumer
from src.exceptions import CoordinatorError
from src.producer import Producer

# Configure module logger
logger = logging.getLogger(__name__)


class Coordinator:
    """
    Orchestrator for the producer-consumer system.

    The Coordinator manages the complete lifecycle of a producer-consumer system:
    - Initializes and starts producer and consumer threads
    - Monitors system execution and collects metrics
    - Coordinates graceful shutdown using sentinel pattern
    - Ensures all threads complete before returning control

    This class provides a clean, high-level interface for running producer-consumer
    workloads while handling the complexity of thread management and synchronization.

    Attributes:
        config: Configuration controlling coordinator behavior
        producers: List of Producer instances to manage
        consumers: List of Consumer instances to manage
        metrics: System-wide metrics collected during execution
    """

    def __init__(
        self,
        config: CoordinatorConfig,
        producers: Optional[List[Producer]] = None,
        consumers: Optional[List[Consumer]] = None,
    ) -> None:
        """
        Initialize a new Coordinator.

        Args:
            config: Configuration object controlling behavior
            producers: List of Producer instances (default: empty list)
            consumers: List of Consumer instances (default: empty list)
        """
        self.config = config
        self.producers = producers or []
        self.consumers = consumers or []
        self.metrics = SystemMetrics()

        logger.info(
            f"Coordinator '{self.config.name}' initialized with "
            f"{len(self.producers)} producer(s) and {len(self.consumers)} consumer(s)"
        )

    def add_producer(self, producer: Producer) -> None:
        """
        Add a producer to the coordinator.

        Args:
            producer: Producer instance to add

        Raises:
            CoordinatorError: If attempting to add producer while system is running
        """
        if any(p.is_running for p in self.producers):
            raise CoordinatorError(
                "Cannot add producer while system is running"
            )

        self.producers.append(producer)
        logger.debug(
            f"Added producer '{producer.config.name}' to coordinator "
            f"'{self.config.name}'"
        )

    def add_consumer(self, consumer: Consumer) -> None:
        """
        Add a consumer to the coordinator.

        Args:
            consumer: Consumer instance to add

        Raises:
            CoordinatorError: If attempting to add consumer while system is running
        """
        if any(c.is_running for c in self.consumers):
            raise CoordinatorError(
                "Cannot add consumer while system is running"
            )

        self.consumers.append(consumer)
        logger.debug(
            f"Added consumer '{consumer.config.name}' to coordinator "
            f"'{self.config.name}'"
        )

    def run(self) -> SystemMetrics:
        """
        Start the producer-consumer system and wait for completion.

        This method:
        1. Records start time
        2. Starts all consumer threads
        3. Starts all producer threads
        4. Waits for all threads to complete
        5. Collects and returns metrics

        The order (consumers first, then producers) ensures consumers are ready
        to receive items before producers start generating them.

        Returns:
            SystemMetrics object containing execution statistics

        Raises:
            CoordinatorError: If system is already running or if startup fails
        """
        if any(p.is_running for p in self.producers) or any(
            c.is_running for c in self.consumers
        ):
            raise CoordinatorError(
                f"Coordinator '{self.config.name}' is already running"
            )

        if not self.producers:
            raise CoordinatorError("Cannot run: no producers configured")

        if not self.consumers:
            raise CoordinatorError("Cannot run: no consumers configured")

        logger.info(
            f"Coordinator '{self.config.name}' starting "
            f"{len(self.producers)} producer(s) and {len(self.consumers)} consumer(s)"
        )

        try:
            # Record start time
            self.metrics.start_time = time.time()

            # Start consumers first (ensure they're ready to receive items)
            for consumer in self.consumers:
                consumer.start()

            # Small delay to ensure consumers are ready
            time.sleep(0.1)

            # Start producers
            for producer in self.producers:
                producer.start()

            # Wait for all threads to complete
            self._wait_for_completion()

            # Record end time and calculate duration
            self.metrics.end_time = time.time()
            self.metrics.calculate_duration()

            # Collect metrics from producers and consumers
            self._collect_metrics()

            logger.info(
                f"Coordinator '{self.config.name}' completed successfully. "
                f"{self.metrics}"
            )

            return self.metrics

        except Exception as e:
            logger.error(
                f"Coordinator '{self.config.name}' failed: {e}",
                exc_info=True,
            )
            self._emergency_shutdown()
            raise CoordinatorError(f"Coordinator failed: {e}") from e

    def _wait_for_completion(self) -> None:
        """
        Wait for all producer and consumer threads to complete.

        This method waits for producers first (they send the sentinel),
        then waits for consumers (they receive the sentinel and terminate).

        Raises:
            CoordinatorError: If threads fail to complete within timeout
        """
        # Wait for all producers to complete
        logger.debug(
            f"Coordinator '{self.config.name}' waiting for producers to complete"
        )
        for producer in self.producers:
            completed = producer.join(timeout=self.config.join_timeout)
            if not completed:
                logger.warning(
                    f"Producer '{producer.config.name}' did not complete within "
                    f"timeout ({self.config.join_timeout}s)"
                )

        # Wait for all consumers to complete
        logger.debug(
            f"Coordinator '{self.config.name}' waiting for consumers to complete"
        )
        for consumer in self.consumers:
            completed = consumer.join(timeout=self.config.join_timeout)
            if not completed:
                logger.warning(
                    f"Consumer '{consumer.config.name}' did not complete within "
                    f"timeout ({self.config.join_timeout}s)"
                )

        # Verify all threads have stopped
        still_running = []
        for producer in self.producers:
            if producer.is_running:
                still_running.append(f"Producer '{producer.config.name}'")

        for consumer in self.consumers:
            if consumer.is_running:
                still_running.append(f"Consumer '{consumer.config.name}'")

        if still_running:
            raise CoordinatorError(
                f"Threads still running after timeout: {', '.join(still_running)}"
            )

    def _collect_metrics(self) -> None:
        """
        Collect metrics from all producers and consumers.

        Aggregates item counts and error counts from all threads into
        the coordinator's metrics object.
        """
        self.metrics.items_produced = sum(
            p.items_produced for p in self.producers
        )
        self.metrics.items_consumed = sum(
            c.items_consumed for c in self.consumers
        )
        self.metrics.producer_errors = sum(
            p.errors_encountered for p in self.producers
        )
        self.metrics.consumer_errors = sum(
            c.errors_encountered for c in self.consumers
        )

        logger.debug(
            f"Coordinator '{self.config.name}' collected metrics: {self.metrics}"
        )

    def _emergency_shutdown(self) -> None:
        """
        Attempt to stop all threads in case of emergency.

        This method is called when an error occurs during normal execution.
        It signals all threads to stop and waits for them to complete.
        """
        logger.warning(
            f"Coordinator '{self.config.name}' initiating emergency shutdown"
        )

        # Signal all threads to stop
        for producer in self.producers:
            try:
                producer.stop()
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")

        for consumer in self.consumers:
            try:
                consumer.stop()
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")

        # Wait for graceful shutdown
        time.sleep(self.config.shutdown_grace_period)

        # Final join attempt
        for producer in self.producers:
            producer.join(timeout=self.config.join_timeout)

        for consumer in self.consumers:
            consumer.join(timeout=self.config.join_timeout)

    def stop(self) -> None:
        """
        Signal all threads to stop gracefully.

        This method can be called to stop the system before all items
        are processed. It signals both producers and consumers to stop.
        """
        logger.info(f"Coordinator '{self.config.name}' stop requested")

        for producer in self.producers:
            producer.stop()

        for consumer in self.consumers:
            consumer.stop()

    def reset(self) -> None:
        """
        Reset the coordinator for a new run.

        This clears the metrics and prepares the coordinator for another
        execution. All threads must be stopped before calling this method.

        Raises:
            CoordinatorError: If any threads are still running
        """
        if any(p.is_running for p in self.producers) or any(
            c.is_running for c in self.consumers
        ):
            raise CoordinatorError(
                "Cannot reset while threads are running"
            )

        self.metrics = SystemMetrics()
        logger.debug(f"Coordinator '{self.config.name}' reset")

