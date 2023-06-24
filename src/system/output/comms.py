from common.comms_base import SystemCommunicationBase, ReliableReceive
from common.messages.stats import StatsRecord
from common.persistence import StatePersistor

from .config import Config


class SystemCommunication(ReliableReceive[StatsRecord], SystemCommunicationBase):
    def __init__(self) -> None:
        super().__init__(Config())

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(Config().in_queue)

    def _process_message(
        self, message: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        super()._process_message(message, queue, delivery_tag, redelivered)
        StatePersistor().save()
