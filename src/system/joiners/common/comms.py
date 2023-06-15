from typing import Callable, Generic

from common.messages import End, Message
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
    out_exchange: str
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        self.out_exchange = self.config.out_exchange
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in
        other_queue = self.config.in_other_queue_format.format(host_id=self.id)
        self.channel.queue_declare(other_queue)  # for station, tripstart & end
        for rk in self.config.in_other_queue_routing_keys:
            self.channel.queue_bind(other_queue, self.config.in_exchange, rk)

        self._start_consuming_from(other_queue)

    def _get_routing_details(
        self, msg: Message[GenericJoinedTrip | End]
    ) -> tuple[str, str]:
        return self.out_exchange, msg.get_routing_key()

    def start_consuming_trips(self, job_id: str) -> None:
        self._start_consuming_from(self.__trips_queue(job_id))

    def stop_consuming_trips(self, job_id: str) -> None:
        self.channel.queue_delete(self.__trips_queue(job_id), if_empty=True)

    def set_all_trips_done_callback(
        self, job_id: str, callback: Callable[[], None]
    ) -> None:
        self._set_empty_queue_callback(self.__trips_queue(job_id), callback)

    def __trips_queue(self, job_id: str) -> str:
        return self.config.in_trips_queue_format.format(job_id=job_id)
