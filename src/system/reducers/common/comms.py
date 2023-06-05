from typing import Generic

from common.comms_base import CommsSendBatched, CommsReceive, SystemCommunicationBase
from common.messages import Message, End
from common.messages.aggregated import GenericAggregatedRecord
from common.messages.stats import StatsRecord

from .config import Config


class ReducerComms(
    Generic[GenericAggregatedRecord],
    CommsReceive[Message[GenericAggregatedRecord | End]],
    CommsSendBatched[Message[GenericAggregatedRecord | End], Message[StatsRecord]],
    SystemCommunicationBase,
):
    OUT_QUEUE = "stats"
    input_queue: str

    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        self.input_queue = f"{config.name}_aggregated"
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.input_queue)

    def _get_routing_details(self, msg: Message[StatsRecord]) -> tuple[str, str]:
        return "", self.OUT_QUEUE
