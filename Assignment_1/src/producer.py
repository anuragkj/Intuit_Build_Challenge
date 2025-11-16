"""
Producer implementation for the producer-consumer pattern.

This module implements the Producer class which reads items from a source
container and places them into a shared queue. The implementation is
queue-agnostic and works with any queue satisfying the QueueProtocol.
"""

import logging
import time
from threading import Thread
from typing import Any, Iterable, Optional

from src.config import ProducerConfig
from src.exceptions import ProducerError, QueueFull
from src.queue_interface import QueueProtocol

# Configure module logger
logger = logging.getLogger(__name__)


class Producer:
    """
    Producer thread that reads from a source and enqueues items.

    The Producer follows these principles:
    - Queue-agnostic: Works with any QueueProtocol implementation
    - Configurable: Behavior controlled via ProducerConfig
    - Observable: Provides metrics and status tracking
    - Resilient: Handles errors gracefully based on configuration
    - Clean Shutdown: Sends sentinel value to signal completion

    The producer operates in its own thread and terminates when:
    1. All source items have been produced
    2. A fatal error occurs (if stop_on_error=True)
    3. The thread is explicitly stopped

    Attributes:
        config: Configuration controlling producer behavior
        source: Iterable providing items to produce
        queue: Thread-safe queue for placing produced items
        sentinel: Special value sent to signal end of production
    """

    def __init__(
        self,
        config: ProducerConfig,
        source: Iterable[Any],
        queue: QueueProtocol[Any],
        sentinel: Optional[Any] = None,
    ) -> None:
        """
        Initialize a new Producer.

        Args:
            config: Configuration object controlling behavior
            source: Iterable providing items to produce
            queue: Thread-safe queue implementing QueueProtocol
            sentinel: Special value to send when production completes (default: None)
        """
        self.config = config
        self.source = source
        self.queue = queue
        self.sentinel = sentinel

        # Metrics and state tracking
        self._items_produced = 0
        self._errors_encountered = 0
        self._is_running = False
        self._thread: Optional[Thread] = None

        logger.debug(
            f"Producer '{self.config.name}' initialized with sentinel={self.sentinel}"
        )

    def start(self) -> None:
        """
        Start the producer thread.

        Creates and starts a new daemon thread that runs the production loop.
        The thread will automatically terminate when the main program exits.

        Raises:
            ProducerError: If the producer is already running.
        """
        if self._is_running:
            raise ProducerError(f"Producer '{self.config.name}' is already running")

        self._thread = Thread(
            target=self._run, name=self.config.name, daemon=True
        )
        self._is_running = True
        self._thread.start()
        logger.info(f"Producer '{self.config.name}' started")

    def join(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for the producer thread to complete.

        Args:
            timeout: Maximum seconds to wait (None = wait indefinitely)

        Returns:
            True if thread completed, False if timeout occurred
        """
        if self._thread is None:
            return True

        self._thread.join(timeout=timeout)
        completed = not self._thread.is_alive()

        if completed:
            logger.debug(f"Producer '{self.config.name}' joined successfully")
        else:
            logger.warning(
                f"Producer '{self.config.name}' join timed out after {timeout}s"
            )

        return completed

    def _run(self) -> None:
        """
        Main production loop executed in the producer thread.

        This method iterates over the source, enqueuing each item. After all
        items are produced, it sends the sentinel value to signal completion.
        Error handling is controlled by the configuration.
        """
        logger.info(
            f"Producer '{self.config.name}' starting production from source"
        )

        try:
            # Produce all items from source
            for item in self.source:
                if not self._is_running:
                    logger.info(
                        f"Producer '{self.config.name}' stopped before completion"
                    )
                    break

                try:
                    self._produce_item(item)
                except Exception as e:
                    self._errors_encountered += 1
                    logger.error(
                        f"Producer '{self.config.name}' error producing item: {e}",
                        exc_info=True,
                    )

                    if self.config.stop_on_error:
                        logger.error(
                            f"Producer '{self.config.name}' stopping due to error"
                        )
                        raise ProducerError(
                            f"Producer stopped due to error: {e}"
                        ) from e

                # Optional delay between items (simulates processing time)
                if self.config.delay_between_items > 0:
                    time.sleep(self.config.delay_between_items)

            # Send sentinel to signal completion
            self._send_sentinel()

            logger.info(
                f"Producer '{self.config.name}' completed. "
                f"Produced {self._items_produced} items, "
                f"errors: {self._errors_encountered}"
            )

        except Exception as e:
            logger.error(
                f"Producer '{self.config.name}' failed with exception: {e}",
                exc_info=True,
            )
            raise
        finally:
            self._is_running = False

    def _produce_item(self, item: Any) -> None:
        """
        Enqueue a single item to the queue.

        Args:
            item: The item to enqueue

        Raises:
            QueueFull: If queue is full and timeout expires
        """
        try:
            self.queue.put(
                item, block=True, timeout=self.config.put_timeout
            )
            self._items_produced += 1

            # Log at debug level for normal items, reduces log verbosity
            if self._items_produced % 100 == 0:  # Log every 100 items
                logger.info(
                    f"Producer '{self.config.name}' produced {self._items_produced} items "
                    f"(queue size: {self.queue.qsize()})"
                )
            else:
                logger.debug(
                    f"Producer '{self.config.name}' produced item: {item} "
                    f"(queue size: {self.queue.qsize()})"
                )

        except QueueFull as e:
            logger.error(
                f"Producer '{self.config.name}' failed to enqueue item "
                f"(timeout={self.config.put_timeout}s): {e}"
            )
            raise

    def _send_sentinel(self) -> None:
        """
        Send sentinel value to signal end of production.

        The sentinel is sent with blocking (no timeout) to ensure it's delivered.
        This guarantees consumers receive the shutdown signal.
        """
        try:
            self.queue.put(self.sentinel, block=True, timeout=None)
            logger.info(
                f"Producer '{self.config.name}' sent sentinel value to signal completion"
            )
        except Exception as e:
            logger.error(
                f"Producer '{self.config.name}' failed to send sentinel: {e}",
                exc_info=True,
            )
            raise ProducerError(f"Failed to send sentinel: {e}") from e

    def stop(self) -> None:
        """
        Signal the producer to stop.

        This sets a flag that will be checked in the production loop. The
        producer will stop after finishing the current item.
        """
        logger.info(f"Producer '{self.config.name}' received stop signal")
        self._is_running = False

    @property
    def items_produced(self) -> int:
        """Get the number of items successfully produced."""
        return self._items_produced

    @property
    def errors_encountered(self) -> int:
        """Get the number of errors encountered during production."""
        return self._errors_encountered

    @property
    def is_running(self) -> bool:
        """Check if the producer is currently running."""
        return self._is_running

