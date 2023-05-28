from common.comms_base import CommsSend, CommsReceive, SystemCommunicationBase
from common.messages.aggregated import PartialRainRecords
from common.messages.stats import StatsRecord

from ..common.comms import ReducerComms


class SystemCommunication(
    CommsReceive[PartialRainRecords],
    CommsSend[StatsRecord],
    SystemCommunicationBase,
    ReducerComms[PartialRainRecords],
):
    INPUT_QUEUE = "rain_aggregated"
    OUT_QUEUE = "stats"

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.INPUT_QUEUE)

    def _get_routing_details(self, record: StatsRecord) -> tuple[str, str]:
        return "", self.OUT_QUEUE
