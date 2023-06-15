from typing import Generic

from common.comms_base import ReliableSend, ReliableReceive, SystemCommunicationBase
from common.messages import Message, End
from common.messages.aggregated import GenericAggregatedRecord
from common.messages.stats import StatsRecord

from .config import Config


class ReducerComms(
    Generic[GenericAggregatedRecord],
    ReliableReceive[Message[GenericAggregatedRecord | End]],
    ReliableSend[Message[StatsRecord]],
    SystemCommunicationBase,
):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.config.in_queue)

    def _get_routing_details(self, msg: Message[StatsRecord]) -> tuple[str, str]:
        return self.config.out_exchange, self.config.out_queue
