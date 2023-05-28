from uuid import uuid4
from typing import Callable

from common.comms_base import (
    CommsReceive,
    SystemCommunicationBase,
    CommsSendBatched,
)
from common.messages import RecordType
from common.messages.joined import JoinedRainRecords
from common.messages.basic import BasicRecord


class SystemCommunication(
    CommsReceive[BasicRecord],
    CommsSendBatched[BasicRecord, JoinedRainRecords],
    SystemCommunicationBase,
):
    EXCHANGE = "basic_records"
    TRIPS_QUEUE = "rain_basic_trips"
    OTHER_QUEUE = f"rain_joiner_other_{uuid4()}"
    OUT_EXCHANGE = "rain_joined_records"

    def _load_definitions(self):
        # in

        self.channel.queue_declare(
            self.OTHER_QUEUE, exclusive=True
        )  # for weather, tripstart & end
        self.channel.queue_bind(
            self.OTHER_QUEUE, self.EXCHANGE, f"{RecordType.WEATHER}.#"
        )
        self.channel.queue_bind(self.OTHER_QUEUE, self.EXCHANGE, RecordType.TRIPS_START)
        self.channel.queue_bind(self.OTHER_QUEUE, self.EXCHANGE, RecordType.END)
        self._start_consuming_from(self.OTHER_QUEUE)

    def _get_routing_details(self, record: JoinedRainRecords):
        return self.OUT_EXCHANGE, record.get_routing_key()

    def start_consuming_trips(self):
        self._start_consuming_from(self.TRIPS_QUEUE)

    def set_all_trips_done_callback(self, callback: Callable[[], None]):
        self._set_empty_queue_callback(self.TRIPS_QUEUE, callback)
