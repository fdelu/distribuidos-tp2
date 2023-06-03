from typing import Callable

from common.comms_base import CommsSendBatched, CommsReceive, SystemCommunicationBase
from common.messages import Message
from common.messages.joined import JoinedRainRecords
from common.messages.aggregated import PartialRainRecords

from ..common.comms import load_definitions


class SystemCommunication(
    CommsReceive[Message[JoinedRainRecords]],
    CommsSendBatched[Message[JoinedRainRecords], Message[PartialRainRecords]],
    SystemCommunicationBase,
):
    NAME = "rain"

    trips_queue: str
    out_queue: str

    def _load_definitions(self) -> None:
        # in
        self.trips_queue, self.out_queue = load_definitions(self, self.NAME)

    def _get_routing_details(self, msg: Message[PartialRainRecords]) -> tuple[str, str]:
        return "", self.out_queue

    def set_all_trips_done_callback(self, callback: Callable[[], None]) -> None:
        self._set_empty_queue_callback(self.trips_queue, callback)
