from common.comms_base import SystemCommunicationBase, CommsReceive
from common.messages.stats import StatsRecord


class SystemCommunication(CommsReceive[StatsRecord], SystemCommunicationBase):
    STATS_QUEUE = "stats"

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.STATS_QUEUE)
