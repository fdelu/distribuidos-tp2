from common.comms_base import CommsSendBatched, CommsReceive, SystemCommunicationBase
from common.messages import Message
from common.messages.aggregated import PartialCityRecords
from common.messages.stats import StatsRecord


class SystemCommunication(
    CommsReceive[Message[PartialCityRecords]],
    CommsSendBatched[Message[PartialCityRecords], Message[StatsRecord]],
    SystemCommunicationBase,
):
    INPUT_QUEUE = "city_aggregated"
    OUT_QUEUE = "stats"

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.INPUT_QUEUE)

    def _get_routing_details(self, msg: Message[StatsRecord]) -> tuple[str, str]:
        return "", self.OUT_QUEUE
