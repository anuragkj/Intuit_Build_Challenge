"""
Custom exception classes for the producer-consumer implementation.

This module defines exception types used throughout the queue and threading operations
to provide clear error handling and debugging information.
"""


class QueueException(Exception):
    """Base exception class for all queue-related errors."""

    pass


class QueueEmpty(QueueException):
    """
    Exception raised when attempting to get from an empty queue.

    This exception is raised when a non-blocking get operation is attempted
    on an empty queue, or when a blocking get operation times out.
    """

    pass


class QueueFull(QueueException):
    """
    Exception raised when attempting to put to a full queue.

    This exception is raised when a non-blocking put operation is attempted
    on a full queue, or when a blocking put operation times out.
    """

    pass


class QueueTimeout(QueueException):
    """
    Exception raised when a queue operation times out.

    This exception is raised when a blocking operation (put or get) exceeds
    the specified timeout duration without completing.
    """

    pass


class ProducerError(Exception):
    """Base exception class for producer-related errors."""

    pass


class ConsumerError(Exception):
    """Base exception class for consumer-related errors."""

    pass


class CoordinatorError(Exception):
    """Base exception class for coordinator-related errors."""

    pass
