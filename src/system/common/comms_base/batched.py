from abc import abstractmethod
from dataclasses import dataclass
import time
from typing import Generic, Protocol, TypeVar

from common.serde.internal.serialize import serialize

from ..config_base import ConfigProtocol

from .protocol import CommsSendProtocol, CommsReceiveProtocol, IN, OUT, BATCH_SEPARATOR
from .util import get_generic_type


class BatchConfig(ConfigProtocol, Protocol):
    batch_size: int


OUT_INNER = TypeVar("OUT_INNER")
MAX_DELAY_S = 3.0


class CommsSendBatched(
    CommsSendProtocol[OUT], CommsReceiveProtocol[IN], Generic[IN, OUT]
):
    """
    Comms with send batching capabilities. If a message is sent with a different
    routing key than a previous message, the previous batch will be sent first,
    even if it is not full.
    """

    batch_size: int
    batches: dict[tuple[str, str], "BatchInfo"]  # exchange, routing_key -> batch
    out_type: type

    def __init__(self, config: BatchConfig):
        super().__init__(config)
        self.batch_size = config.batch_size
        self.batches = {}
        self.out_type = get_generic_type(self, CommsSendBatched, 1)
        self.add_stop_callback(self.__flush)

    def send(self, record: OUT):
        key = self._get_routing_details(record)
        if self.is_stopped():
            # I must send it now since I won't be able to set a timer
            # to check later
            self.__send_batch(*key, [record])

        now = time.time()
        batch = self.batches.setdefault(key, self.BatchInfo([], now))
        if len(self.batches) == 0:
            self.set_timer(self.__check_delay, MAX_DELAY_S)
        batch.records.append(record)

        if len(batch.records) < self.batch_size:
            return

        # Make sure to send the batches with routing
        # keys that called .send() before first
        self.__send_up_to(key)

    def __check_delay(self) -> None:
        """
        Periodic timer that checks if the maximum delay has passed,
        and sends all batches
        """
        now = time.time()

        key = next(
            (k for k, b in self.batches.items() if now - b.first_on >= MAX_DELAY_S),
            None,
        )
        if key is not None:
            self.__send_up_to(key)
            return

        next_batch = next(iter(self.batches.values()), None)
        if next_batch is None:
            remaining = MAX_DELAY_S
        else:
            remaining = MAX_DELAY_S - (now - next_batch.first_on)

        self.set_timer(self.__check_delay, remaining)

    def __flush(self):
        """
        Sends all batches that are currently in the queue
        """
        for batch_key, batch in self.batches.items():
            self.__send_batch(*batch_key, batch.records)
        self.batches.clear()

    def __send_up_to(self, key: tuple[str, str]):
        """
        Sends all batches up to the given key
        """
        for batch_key in list(self.batches.keys()):
            self.__send_batch(*batch_key, self.batches.pop(batch_key).records)
            if batch_key == key:
                break

    def __send_batch(self, exchange: str, routing_key: str, batch: list[OUT]):
        """
        Sends the given batch to the given exchange and routing key
        """
        msg = BATCH_SEPARATOR.join(self._serialize_record(record) for record in batch)
        self.channel.basic_publish(exchange, routing_key, msg.encode())

    def _serialize_record(self, message: OUT) -> str:
        return serialize(message, set_type=self.out_type)

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        ...

    @dataclass
    class BatchInfo(Generic[OUT_INNER]):
        records: list[OUT_INNER]
        first_on: float
