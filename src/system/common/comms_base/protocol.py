from abc import abstractmethod
from typing import Callable, Protocol, TypeVar

from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

from ..config_base import ConfigProtocol

IN = TypeVar("IN", covariant=True)
OUT = TypeVar("OUT", contravariant=True)


TIMEOUT_SECONDS = 1  # default timeout when receiving messages

__all__ = ["ConfigProtocol"]


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

    @property
    @abstractmethod
    def id(self) -> str:
        """
        Returns an unique id for this instance that
        is persistent between restarts
        """
        ...

    @abstractmethod
    def __init__(self, config: ConfigProtocol) -> None:
        ...


class ReceiveReliableProtocol(Protocol):
    @abstractmethod
    def current_message_id(self) -> str | None:
        """
        Returns the current message id if any
        """
        ...

    def set_batch_done_callback(self, callback: Callable[[], None]) -> None:
        """
        Sets a callback to be called when a batch is done
        """
        ...
