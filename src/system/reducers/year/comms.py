from common.messages import Message
from common.comms_base import CommsSend, CommsReceive, SystemCommunicationBase
from common.messages.aggregated import PartialYearRecords
from common.messages.stats import StatsRecord

from ..common.comms import ReducerComms


class SystemCommunication(
    CommsReceive[PartialYearRecords],
    CommsSend[StatsRecord],
    SystemCommunicationBase,
    ReducerComms[PartialYearRecords],
):
    INPUT_QUEUE = "year_aggregated"
    OUT_QUEUE = "stats"

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.INPUT_QUEUE)

    def _get_routing_details(self, msg: Message[StatsRecord]) -> tuple[str, str]:
        return "", self.OUT_QUEUE
