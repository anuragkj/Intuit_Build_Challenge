"""
Producer-Consumer Pattern Implementation.

This package provides a comprehensive implementation of the producer-consumer pattern
with both custom and standard library queue implementations, demonstrating thread
synchronization, concurrent programming, and blocking queue operations.
"""

__version__ = "1.0.0"
__author__ = "Intuit Build Challenge"

from src.config import ConsumerConfig, CoordinatorConfig, ProducerConfig
from src.consumer import Consumer
from src.coordinator import Coordinator
from src.custom_queue import ThreadSafeQueue
from src.exceptions import QueueEmpty, QueueFull, QueueTimeout
from src.producer import Producer
from src.queue_interface import QueueProtocol

__all__ = [
    "ThreadSafeQueue",
    "QueueProtocol",
    "QueueEmpty",
    "QueueFull",
    "QueueTimeout",
    "Producer",
    "Consumer",
    "Coordinator",
    "ProducerConfig",
    "ConsumerConfig",
    "CoordinatorConfig",
]

