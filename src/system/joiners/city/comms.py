from typing import Callable
from uuid import uuid4

from common.comms_base import (
    CommsSendBatched,
    SystemCommunicationBase,
    CommsReceive,
)
from common.messages import RecordType, Message
from common.messages.basic import BasicRecord
from common.messages.joined import JoinedCityRecords

from .config import Config


class SystemCommunication(
    CommsReceive[BasicRecord],
    CommsSendBatched[BasicRecord, JoinedCityRecords],
    SystemCommunicationBase,
):
    EXCHANGE = "basic_records"
    TRIPS_QUEUE = "city_basic_trips"
    OTHER_QUEUE = f"city_joiner_other_{uuid4()}"
    OUT_EXCHANGE = "city_joined_records"

    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in

        self.channel.queue_declare(
            self.OTHER_QUEUE, exclusive=True
        )  # for station, tripstart & end
        self.__bind_city(self.OTHER_QUEUE, RecordType.STATION)
        self.channel.queue_bind(self.OTHER_QUEUE, self.EXCHANGE, RecordType.TRIPS_START)
        self.channel.queue_bind(self.OTHER_QUEUE, self.EXCHANGE, RecordType.END)

        self.__bind_city(self.TRIPS_QUEUE, RecordType.TRIP)
        self._start_consuming_from(self.OTHER_QUEUE)

    def __bind_city(self, queue: str, record: RecordType) -> None:
        self.channel.queue_bind(queue, self.EXCHANGE, f"{record}.{self.config.city}.*")

    def _get_routing_details(self, msg: Message[JoinedCityRecords]) -> tuple[str, str]:
        return self.OUT_EXCHANGE, msg.payload.get_routing_key()

    def start_consuming_trips(self) -> None:
        self._start_consuming_from(self.TRIPS_QUEUE)

    def set_all_trips_done_callback(self, callback: Callable[[], None]) -> None:
        self._set_empty_queue_callback(self.TRIPS_QUEUE, callback)
