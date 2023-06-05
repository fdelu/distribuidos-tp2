from uuid import uuid4
from typing import Callable, Generic

from common.comms_base import CommsSendBatched, CommsReceive, SystemCommunicationBase
from common.messages import Message, End, RecordType
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config


class AggregatorComms(
    Generic[GenericJoinedTrip, GenericAggregatedRecord],
    CommsReceive[Message[GenericJoinedTrip | End]],
    CommsSendBatched[
        Message[GenericJoinedTrip | End], Message[GenericAggregatedRecord | End]
    ],
    SystemCommunicationBase,
):
    exchange: str
    trips_queue: str
    end_queue: str
    out_queue: str

    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        self.exchange = f"{config.name}_joined_records"
        self.trips_queue = f"{config.name}_joined_trips"
        self.end_queue = f"{config.name}_aggregators_ends_{uuid4()}"
        self.out_queue = f"{config.name}_aggregated"
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.trips_queue)

        self.channel.queue_declare(self.end_queue, exclusive=True)  # end
        self.channel.queue_bind(self.end_queue, self.exchange, RecordType.END)
        self._start_consuming_from(self.end_queue)

    def _get_routing_details(
        self, msg: Message[GenericAggregatedRecord | End]
    ) -> tuple[str, str]:
        return "", self.out_queue

    def set_all_trips_done_callback(self, callback: Callable[[], None]) -> None:
        self._set_empty_queue_callback(self.trips_queue, callback)
