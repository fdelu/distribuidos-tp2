from abc import abstractmethod
from typing import Any, Callable, Protocol, TypeVar

from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel


from ..config_base import ConfigProtocol

IN = TypeVar("IN", covariant=True)
OUT = TypeVar("OUT", contravariant=True)


BATCH_SEPARATOR = "\n"  # separator for batched messages
TIMEOUT_SECONDS = 1  # default timeout when receiving messages


# Base comms
class CommsProtocol(Protocol):
    @property
    @abstractmethod
    def connection(self) -> BlockingConnection:
        """
        Middleware connection
        """
        ...

    @property
    @abstractmethod
    def channel(self) -> BlockingChannel:
        """
        Middleware channel
        """
        ...

    @abstractmethod
    def __init__(self, config: ConfigProtocol) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Closes the connection
        """
        ...


# Comms with receive capabilities
class CommsReceiveProtocol(CommsProtocol, Protocol[IN]):
    @abstractmethod
    def start_consuming(self) -> None:
        """
        Start consuming messages from the queues
        """
        ...

    @abstractmethod
    def stop_consuming(self) -> None:
        """
        Signal/Thread-Safe way to stop consuming
        """
        ...

    @abstractmethod
    def set_callback(self, callback: Callable[[IN], None]) -> None:
        """
        Sets the callback to be called when a record is received
        """
        ...

    @abstractmethod
    def is_stopped(self) -> bool:
        """
        Returns whether the consuming was stopped (or will be soon)
        """
        ...

    @abstractmethod
    def set_timer(self, callback: Callable[[], None], timeout_seconds: float) -> Any:
        """
        Calls the callback after timeout seconds.
        Returns an object that can be used to cancel the timer.
        """
        ...

    @abstractmethod
    def cancel_timer(self, timer: Any) -> None:
        """
        Cancels the timer
        """
        ...

    def add_stop_callback(self, callback: Callable[[], None]) -> None:
        """
        Sets a callback to be called when the consuming is stopped.
        It won't be called if the consuming was interrupted by a signal.
        """
        ...

    @abstractmethod
    def _load_definitions(self) -> None:
        """
        Declares the exchanges, queues and bindings required for the communication
        """
        ...

    @abstractmethod
    def _start_consuming_from(self, queue: str) -> None:
        """
        Starts consuming from the given queue.
        This method returns when stop_consuming() is called.
        """
        ...

    @abstractmethod
    def _set_timeout_callback(
        self,
        queue: str,
        callback: Callable[[], None],
        timeout: float = TIMEOUT_SECONDS,
    ) -> None:
        """
        Sets a callback to be called after timeout seconds passed since the last message
        of the given queue was received. If no message is received after the call to
        this method, the callback will be called after timeout seconds.
        """
        ...

    @abstractmethod
    def _set_empty_queue_callback(
        self, queue: str, callback: Callable[[], None], **queue_kwargs: Any
    ) -> None:
        """
        Sets a callback to be called when the given queue is empty.
        """
        ...

    def _deserialize_record(self, message: str) -> IN:
        """
        Deserialize the given message into the input type
        """
        ...


# Comms with send capabilities
class CommsSendProtocol(CommsProtocol, Protocol[OUT]):
    @abstractmethod
    def send(self, record: OUT) -> None:
        """
        Sends a record to the appropriate queue
        """
        ...

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        """
        Should return a tuple of (exchange_name, routing_key) for this record
        """
        ...

    @abstractmethod
    def _serialize_record(self, message: OUT) -> str:
        """
        Serializes the given message into a string
        """
        ...
