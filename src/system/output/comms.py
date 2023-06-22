from common.comms_base import SystemCommunicationBase, ReliableReceive
from common.messages import Message
from common.messages.stats import StatsRecord
from common.persistence import StatePersistor

from .config import Config


class SystemCommunication(
    ReliableReceive[Message[StatsRecord]], SystemCommunicationBase
):
    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.config.in_queue)

    def _process_message(
        self, message: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        super()._process_message(message, delivery_tag, redelivered)
        StatePersistor().save()
