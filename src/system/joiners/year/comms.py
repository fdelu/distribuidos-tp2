from uuid import uuid4
from typing import Callable

from common.comms_base import (
    CommsReceive,
    SystemCommunicationBase,
    CommsSendBatched,
)
from common.messages import RecordType
from common.messages.joined import JoinedYearRecords
from common.messages.basic import BasicRecord

from .config import Config


class SystemCommunication(
    CommsReceive[BasicRecord],
    CommsSendBatched[BasicRecord, JoinedYearRecords],
    SystemCommunicationBase,
):
    EXCHANGE = "basic_records"
    TRIPS_QUEUE = "year_basic_trips"
    OTHER_QUEUE = f"year_joiner_other_{uuid4()}"
    OUT_EXCHANGE = "year_joined_records"

    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in

        self.channel.queue_declare(
            self.OTHER_QUEUE, exclusive=True
        )  # for station, tripstart & end
        self.__bind_years(self.OTHER_QUEUE, RecordType.STATION)
        self.channel.queue_bind(self.OTHER_QUEUE, self.EXCHANGE, RecordType.TRIPS_START)
        self.channel.queue_bind(self.OTHER_QUEUE, self.EXCHANGE, RecordType.END)

        self.__bind_years(self.TRIPS_QUEUE, RecordType.TRIP)
        self._start_consuming_from(self.OTHER_QUEUE)

    def __bind_years(self, queue: str, record: RecordType) -> None:
        for year in (self.config.year_base, self.config.year_compared):
            self.channel.queue_bind(queue, self.EXCHANGE, f"{record}.*.{year}")

    def _get_routing_details(self, record: JoinedYearRecords) -> tuple[str, str]:
        return self.OUT_EXCHANGE, record.get_routing_key()

    def start_consuming_trips(self) -> None:
        self._start_consuming_from(self.TRIPS_QUEUE)

    def set_all_trips_done_callback(self, callback: Callable[[], None]) -> None:
        self._set_empty_queue_callback(self.TRIPS_QUEUE, callback)
