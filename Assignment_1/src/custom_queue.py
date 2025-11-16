"""
Custom thread-safe bounded blocking queue implementation.

This module implements a production-grade thread-safe queue using low-level
synchronization primitives (Lock and Condition) to demonstrate mastery of
concurrent programming concepts including mutual exclusion, condition variables,
and the wait/notify pattern.
"""

import time
from collections import deque
from threading import Condition, Lock
from typing import Generic, Optional, TypeVar

from src.exceptions import QueueEmpty, QueueFull

# Generic type variable for queue items
T = TypeVar("T")


class ThreadSafeQueue(Generic[T]):
    """
    A bounded thread-safe queue implemented using threading primitives.

    This implementation demonstrates low-level thread synchronization using:
    - Lock: For mutual exclusion protecting queue state
    - Condition: For wait/notify pattern when queue is empty or full
    - Deque: For efficient O(1) append and pop operations

    The queue supports:
    - Bounded capacity with configurable maximum size
    - Blocking and non-blocking operations
    - Timeout support for blocking operations
    - Thread-safe concurrent access from multiple producers and consumers

    Attributes:
        _queue: Internal deque storing the queue items
        _maxsize: Maximum number of items allowed (0 = unbounded)
        _lock: Lock protecting all queue operations
        _not_empty: Condition variable signaled when items are added
        _not_full: Condition variable signaled when items are removed
    """

    def __init__(self, maxsize: int = 0) -> None:
        """
        Initialize a new ThreadSafeQueue.

        Args:
            maxsize: Maximum number of items in queue. If maxsize <= 0,
                     the queue size is unbounded.
        """
        self._queue: deque[T] = deque()
        self._maxsize: int = maxsize
        self._lock: Lock = Lock()

        # Condition variables for blocking operations
        # Both share the same underlying lock for atomicity
        self._not_empty: Condition = Condition(self._lock)
        self._not_full: Condition = Condition(self._lock)

    def put(
        self, item: T, block: bool = True, timeout: Optional[float] = None
    ) -> None:
        """
        Put an item into the queue.

        If the queue is full and block is True, this method will wait until
        space becomes available or until the timeout expires. If block is False,
        it will raise QueueFull immediately if the queue is full.

        Thread Safety:
            This method acquires the internal lock and is safe to call from
            multiple threads concurrently.

        Args:
            item: The item to add to the queue
            block: Whether to block if queue is full (default: True)
            timeout: Maximum seconds to wait if blocking (None = wait forever)

        Raises:
            QueueFull: If the queue is full and block=False, or if the timeout
                      expires while waiting for space.

        Example:
            >>> queue = ThreadSafeQueue(maxsize=10)
            >>> queue.put("item1")  # Blocking put
            >>> queue.put("item2", block=False)  # Non-blocking put
            >>> queue.put("item3", timeout=5.0)  # Put with 5 second timeout
        """
        with self._not_full:  # Automatically acquires and releases the lock
            # Check if queue is full
            if self._maxsize > 0:
                if not block:
                    # Non-blocking mode: raise immediately if full
                    if self._qsize() >= self._maxsize:
                        raise QueueFull("Queue is full")
                elif timeout is None:
                    # Blocking mode with no timeout: wait indefinitely
                    while self._qsize() >= self._maxsize:
                        self._not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    # Blocking mode with timeout
                    endtime = time.time() + timeout
                    while self._qsize() >= self._maxsize:
                        remaining = endtime - time.time()
                        if remaining <= 0.0:
                            raise QueueFull(
                                f"Queue is full - timeout after {timeout}s"
                            )
                        self._not_full.wait(remaining)

            # Add item to queue
            self._queue.append(item)

            # Notify one waiting consumer that an item is available
            self._not_empty.notify()

    def get(self, block: bool = True, timeout: Optional[float] = None) -> T:
        """
        Remove and return an item from the queue.

        If the queue is empty and block is True, this method will wait until
        an item becomes available or until the timeout expires. If block is False,
        it will raise QueueEmpty immediately if the queue is empty.

        Thread Safety:
            This method acquires the internal lock and is safe to call from
            multiple threads concurrently.

        Args:
            block: Whether to block if queue is empty (default: True)
            timeout: Maximum seconds to wait if blocking (None = wait forever)

        Returns:
            The item removed from the queue (FIFO order).

        Raises:
            QueueEmpty: If the queue is empty and block=False, or if the timeout
                       expires while waiting for an item.

        Example:
            >>> queue = ThreadSafeQueue()
            >>> queue.put("item1")
            >>> item = queue.get()  # Blocking get
            >>> item = queue.get(block=False)  # Non-blocking get (may raise)
            >>> item = queue.get(timeout=5.0)  # Get with 5 second timeout
        """
        with self._not_empty:  # Automatically acquires and releases the lock
            # Check if queue is empty
            if not block:
                # Non-blocking mode: raise immediately if empty
                if not self._qsize():
                    raise QueueEmpty("Queue is empty")
            elif timeout is None:
                # Blocking mode with no timeout: wait indefinitely
                while not self._qsize():
                    self._not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                # Blocking mode with timeout
                endtime = time.time() + timeout
                while not self._qsize():
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        raise QueueEmpty(f"Queue is empty - timeout after {timeout}s")
                    self._not_empty.wait(remaining)

            # Remove and return item from queue (FIFO order)
            item = self._queue.popleft()

            # Notify one waiting producer that space is available
            self._not_full.notify()

            return item

    def qsize(self) -> int:
        """
        Return the approximate size of the queue.

        This is "approximate" because concurrent operations may change the size
        between when this method checks the size and when it returns.

        Thread Safety:
            This method is thread-safe but the returned value may be stale
            immediately after the method returns due to concurrent access.

        Returns:
            The number of items currently in the queue.
        """
        with self._lock:
            return self._qsize()

    def empty(self) -> bool:
        """
        Return True if the queue is empty, False otherwise.

        Warning:
            This method is unreliable in multi-threaded code. The queue may
            become non-empty immediately after this returns True, or vice versa.
            It's provided for compatibility but should not be relied upon for
            synchronization logic.

        Returns:
            True if the queue is empty, False otherwise.
        """
        with self._lock:
            return not self._qsize()

    def full(self) -> bool:
        """
        Return True if the queue is full, False otherwise.

        Warning:
            This method is unreliable in multi-threaded code. The queue may
            become non-full immediately after this returns True, or vice versa.
            It's provided for compatibility but should not be relied upon for
            synchronization logic.

        Returns:
            True if the queue is full (at max capacity), False otherwise.
            Always returns False for unbounded queues (maxsize <= 0).
        """
        with self._lock:
            return 0 < self._maxsize <= self._qsize()

    def _qsize(self) -> int:
        """
        Internal helper to get queue size without acquiring lock.

        This method assumes the caller has already acquired the lock.
        It exists to avoid redundant lock acquisitions in methods that
        already hold the lock.

        Returns:
            The number of items currently in the queue.
        """
        return len(self._queue)

    def __repr__(self) -> str:
        """Return a string representation of the queue."""
        return (
            f"ThreadSafeQueue(maxsize={self._maxsize}, "
            f"current_size={self.qsize()})"
        )

