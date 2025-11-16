"""
Queue interface definition using Protocol for duck typing.

This module defines the QueueProtocol that both the custom ThreadSafeQueue and
the standard library queue.Queue implement, enabling dependency injection and
interchangeable queue implementations.
"""

from typing import Optional, Protocol, TypeVar

# Generic type variable for queue items
T = TypeVar("T")


class QueueProtocol(Protocol[T]):
    """
    Protocol defining the interface for thread-safe queue implementations.

    This protocol establishes the contract that any queue implementation must
    fulfill to be compatible with the producer-consumer system. Both the custom
    ThreadSafeQueue and Python's queue.Queue satisfy this protocol.

    The protocol ensures type safety through duck typing while maintaining
    flexibility in implementation choices.
    """

    def put(self, item: T, block: bool = True, timeout: Optional[float] = None) -> None:
        """
        Put an item into the queue.

        Args:
            item: The item to add to the queue.
            block: If True, block if queue is full until space is available.
                   If False, raise QueueFull immediately if queue is full.
            timeout: Maximum time in seconds to block. None means block indefinitely.
                     Only meaningful if block=True.

        Raises:
            QueueFull: If queue is full and block=False, or if timeout expires.
            QueueTimeout: If the operation times out (implementation-specific).
        """
        ...

    def get(self, block: bool = True, timeout: Optional[float] = None) -> T:
        """
        Remove and return an item from the queue.

        Args:
            block: If True, block if queue is empty until an item is available.
                   If False, raise QueueEmpty immediately if queue is empty.
            timeout: Maximum time in seconds to block. None means block indefinitely.
                     Only meaningful if block=True.

        Returns:
            The item removed from the queue.

        Raises:
            QueueEmpty: If queue is empty and block=False, or if timeout expires.
            QueueTimeout: If the operation times out (implementation-specific).
        """
        ...

    def qsize(self) -> int:
        """
        Return the approximate size of the queue.

        Note: This is approximate because of concurrent access. The value may
        be stale by the time it's returned in multi-threaded scenarios.

        Returns:
            The approximate number of items in the queue.
        """
        ...

    def empty(self) -> bool:
        """
        Return True if the queue is empty, False otherwise.

        Note: This is not reliable in multi-threaded code. The queue state
        may change immediately after this call returns.

        Returns:
            True if queue appears empty, False otherwise.
        """
        ...

    def full(self) -> bool:
        """
        Return True if the queue is full, False otherwise.

        Note: This is not reliable in multi-threaded code. The queue state
        may change immediately after this call returns.

        Returns:
            True if queue appears full, False otherwise.
        """
        ...
