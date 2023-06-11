from abc import abstractmethod
from typing import Generic, TypeVar

from shared.serde.internal.serialize import serialize

from ...messages import Batch

from ..protocol import CommsProtocol, ReceiveReliableProtocol, OUT, ConfigProtocol


OUT_INNER = TypeVar("OUT_INNER")
MAX_DELAY_S = 3.0


class ReliableSend(CommsProtocol, ReceiveReliableProtocol, Generic[OUT]):
    """
    Comms with send batching capabilities. It will group messages in batches
    with the same routing key and send them when the batch being received is
    done.
    """

    batches: dict[tuple[str, str], "Batch[OUT]"]  # exchange, routing_key -> batch
    routing_count: int = 0

    def __init__(self, config: ConfigProtocol) -> None:
        super().__init__(config)
        self.batches = {}
        self.set_batch_done_callback(self.__flush)

    def send(self, record: OUT) -> None:
        key = self._get_routing_details(record)
        batch = self.batches.get(key) or Batch([], self.__next_message_id())
        batch.messages.append(record)
        if batch.msg_id is None:
            self.__send_batch(*key, batch)
            return
        self.batches[key] = batch

    def __next_message_id(self) -> str | None:
        """
        Returns the next message id to be used
        """
        id = self.current_message_id()
        if id is None:
            return None
        ret = id
        if self.routing_count > 0:
            ret += f";{self.routing_count}"
        self.routing_count += 1
        return ret

    def __flush(self) -> None:
        """
        Sends all batches that are currently in the queue
        """
        for batch_key, batch in self.batches.items():
            self.__send_batch(*batch_key, batch)
        self.batches.clear()
        self.routing_count = 0

    def __send_batch(self, exchange: str, routing_key: str, batch: Batch[OUT]) -> None:
        """
        Sends the given batch to the given exchange and routing key
        """
        msg = serialize(batch)
        self.channel.basic_publish(exchange, routing_key, msg.encode())

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        ...
