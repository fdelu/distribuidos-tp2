from uuid import uuid4
from typing import Callable

from common.comms_base import (
    SystemCommunicationBase,
    CommsReceive,
    CommsSendBatched,
)
from common.messages import RecordType, Message
from common.messages.raw import RawRecord
from common.messages.basic import BasicRecord


class SystemCommunication(
    CommsReceive[Message[RawRecord]],
    CommsSendBatched[Message[RawRecord], Message[BasicRecord]],
    SystemCommunicationBase,
):
    EXCHANGE = "raw_records"
    BATCHS_QUEUE = "raw_batchs"
    END_QUEUE = f"parser_ends_{uuid4()}"
    OUT_EXCHANGE = "basic_records"

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.BATCHS_QUEUE)

        self.channel.queue_declare(self.END_QUEUE, exclusive=True)
        self.channel.queue_bind(self.END_QUEUE, self.EXCHANGE, RecordType.END)
        self._start_consuming_from(self.END_QUEUE)

    def _get_routing_details(self, msg: Message[BasicRecord]) -> tuple[str, str]:
        return self.OUT_EXCHANGE, msg.payload.get_routing_key()

    def set_all_batchs_done_callback(self, callback: Callable[[], None]) -> None:
        self._set_empty_queue_callback(self.BATCHS_QUEUE, callback)
