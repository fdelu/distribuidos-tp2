from typing import TypeVar, Generic
from abc import ABC, abstractmethod

from shared.serde import serialize

from ..protocol import CommsProtocol

OUT = TypeVar("OUT", contravariant=True)


class CommsSend(CommsProtocol, Generic[OUT], ABC):
    """
    Comms with send capabilities. See protocol.py for more details about the methods.
    """

    def send(self, record: OUT) -> None:
        """
        Sends a record to the appropriate queue
        """
        exchange, routing_key = self._get_routing_details(record)
        self.__send_to(record, exchange, routing_key)

    def __send_to(self, record: OUT, exchange: str, routing_key: str) -> None:
        self.channel.basic_publish(
            exchange,
            routing_key,
            serialize(record).encode(),
        )

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        """
        Should return a tuple of (exchange_name, routing_key) for this record
        """
        ...
