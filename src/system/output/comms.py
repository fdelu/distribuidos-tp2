from common.comms_base import SystemCommunicationBase, ReliableReceive, HeartbeatSender
from common.messages import Message
from common.messages.stats import StatsRecord

from .config import Config


class SystemCommunication(
    ReliableReceive[Message[StatsRecord]], SystemCommunicationBase
):
    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)
        HeartbeatSender(self, config).setup_timer()

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.config.in_queue)
