"""
Consumer implementation for the producer-consumer pattern.

This module implements the Consumer class which reads items from a shared
queue and writes them to a destination container. The implementation is
queue-agnostic and works with any queue satisfying the QueueProtocol.
"""

import logging
import time
from threading import Thread
from typing import Any, List, Optional

from src.config import ConsumerConfig
from src.exceptions import ConsumerError, QueueEmpty
from src.queue_interface import QueueProtocol

# Configure module logger
logger = logging.getLogger(__name__)


class Consumer:
    """
    Consumer thread that dequeues items and writes to a destination.

    The Consumer follows these principles:
    - Queue-agnostic: Works with any QueueProtocol implementation
    - Configurable: Behavior controlled via ConsumerConfig
    - Observable: Provides metrics and status tracking
    - Resilient: Handles errors gracefully based on configuration
    - Clean Shutdown: Detects sentinel value to terminate gracefully

    The consumer operates in its own thread and terminates when:
    1. The sentinel value is received from the queue
    2. A fatal error occurs (if stop_on_error=True)
    3. The thread is explicitly stopped

    Attributes:
        config: Configuration controlling consumer behavior
        destination: List to store consumed items
        queue: Thread-safe queue for retrieving items
        sentinel: Special value indicating end of consumption
    """

    def __init__(
        self,
        config: ConsumerConfig,
        destination: List[Any],
        queue: QueueProtocol[Any],
        sentinel: Optional[Any] = None,
    ) -> None:
        """
        Initialize a new Consumer.

        Args:
            config: Configuration object controlling behavior
            destination: List to store consumed items
            queue: Thread-safe queue implementing QueueProtocol
            sentinel: Special value indicating end of consumption (default: None)
        """
        self.config = config
        self.destination = destination
        self.queue = queue
        self.sentinel = sentinel

        # Metrics and state tracking
        self._items_consumed = 0
        self._errors_encountered = 0
        self._is_running = False
        self._thread: Optional[Thread] = None

        logger.debug(
            f"Consumer '{self.config.name}' initialized with sentinel={self.sentinel}"
        )

    def start(self) -> None:
        """
        Start the consumer thread.

        Creates and starts a new daemon thread that runs the consumption loop.
        The thread will automatically terminate when the main program exits.

        Raises:
            ConsumerError: If the consumer is already running.
        """
        if self._is_running:
            raise ConsumerError(f"Consumer '{self.config.name}' is already running")

        self._thread = Thread(
            target=self._run, name=self.config.name, daemon=True
        )
        self._is_running = True
        self._thread.start()
        logger.info(f"Consumer '{self.config.name}' started")

    def join(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for the consumer thread to complete.

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
            logger.debug(f"Consumer '{self.config.name}' joined successfully")
        else:
            logger.warning(
                f"Consumer '{self.config.name}' join timed out after {timeout}s"
            )

        return completed

    def _run(self) -> None:
        """
        Main consumption loop executed in the consumer thread.

        This method continuously dequeues items from the queue and adds them
        to the destination. The loop terminates when the sentinel value is
        received. Error handling is controlled by the configuration.
        """
        logger.info(
            f"Consumer '{self.config.name}' starting consumption to destination"
        )

        try:
            while self._is_running:
                try:
                    # Get item from queue
                    item = self.queue.get(
                        block=True, timeout=self.config.get_timeout
                    )

                    # Check for sentinel (shutdown signal)
                    if item is self.sentinel:
                        logger.info(
                            f"Consumer '{self.config.name}' received sentinel, "
                            f"shutting down gracefully"
                        )
                        break

                    # Consume the item
                    self._consume_item(item)

                except QueueEmpty:
                    # Queue is empty and timeout occurred
                    logger.debug(
                        f"Consumer '{self.config.name}' queue empty timeout, "
                        f"continuing..."
                    )
                    continue

                except Exception as e:
                    self._errors_encountered += 1
                    logger.error(
                        f"Consumer '{self.config.name}' error consuming item: {e}",
                        exc_info=True,
                    )

                    if self.config.stop_on_error:
                        logger.error(
                            f"Consumer '{self.config.name}' stopping due to error"
                        )
                        raise ConsumerError(
                            f"Consumer stopped due to error: {e}"
                        ) from e

                # Optional delay between items (simulates processing time)
                if self.config.delay_between_items > 0:
                    time.sleep(self.config.delay_between_items)

            logger.info(
                f"Consumer '{self.config.name}' completed. "
                f"Consumed {self._items_consumed} items, "
                f"errors: {self._errors_encountered}"
            )

        except Exception as e:
            logger.error(
                f"Consumer '{self.config.name}' failed with exception: {e}",
                exc_info=True,
            )
            raise
        finally:
            self._is_running = False

    def _consume_item(self, item: Any) -> None:
        """
        Process and store a single item.

        Args:
            item: The item to consume and store in destination

        Raises:
            Exception: Any error that occurs during item processing
        """
        try:
            self.destination.append(item)
            self._items_consumed += 1

            # Log at debug level for normal items, reduces log verbosity
            if self._items_consumed % 100 == 0:  # Log every 100 items
                logger.info(
                    f"Consumer '{self.config.name}' consumed {self._items_consumed} items "
                    f"(destination size: {len(self.destination)})"
                )
            else:
                logger.debug(
                    f"Consumer '{self.config.name}' consumed item: {item} "
                    f"(destination size: {len(self.destination)})"
                )

        except Exception as e:
            logger.error(
                f"Consumer '{self.config.name}' failed to store item: {e}",
                exc_info=True,
            )
            raise

    def stop(self) -> None:
        """
        Signal the consumer to stop.

        This sets a flag that will be checked in the consumption loop. The
        consumer will stop after finishing the current item.
        """
        logger.info(f"Consumer '{self.config.name}' received stop signal")
        self._is_running = False

    @property
    def items_consumed(self) -> int:
        """Get the number of items successfully consumed."""
        return self._items_consumed

    @property
    def errors_encountered(self) -> int:
        """Get the number of errors encountered during consumption."""
        return self._errors_encountered

    @property
    def is_running(self) -> bool:
        """Check if the consumer is currently running."""
        return self._is_running

