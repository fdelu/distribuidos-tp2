from typing import Callable, Generic
from uuid import uuid4

from common.messages import End, Message, RecordType
from common.messages.basic import BasicRecord
from common.messages.joined import GenericJoinedTrip
from common.comms_base import (
    ReliableSend,
    SystemCommunicationBase,
    ReliableReceive,
)

from .config import Config

__all__ = ["GenericJoinedTrip"]


class JoinerComms(
    Generic[GenericJoinedTrip],
    ReliableReceive[Message[BasicRecord]],
    ReliableSend[Message[GenericJoinedTrip | End]],
    SystemCommunicationBase,
):
    EXCHANGE = "basic_records"
    trips_queue: str
    other_queue: str
    out_exchange: str

    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        self.trips_queue = f"{config.name}_basic_trips"
        self.other_queue = f"{config.name}_joiner_other_{uuid4()}"
        self.out_exchange = f"{config.name}_joined_records"
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in

        self.channel.queue_declare(
            self.other_queue, exclusive=True
        )  # for station, tripstart & end
        self.channel.queue_bind(self.other_queue, self.EXCHANGE, RecordType.TRIPS_START)
        self.channel.queue_bind(self.other_queue, self.EXCHANGE, RecordType.END)

        self._start_consuming_from(self.other_queue)

    def _get_routing_details(
        self, msg: Message[GenericJoinedTrip | End]
    ) -> tuple[str, str]:
        return self.out_exchange, msg.payload.get_routing_key()

    def start_consuming_trips(self) -> None:
        self._start_consuming_from(self.trips_queue)

    def set_all_trips_done_callback(self, callback: Callable[[], None]) -> None:
        self._set_empty_queue_callback(self.trips_queue, callback)
