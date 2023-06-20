from typing import Callable, Generic

from common.comms_base import ReliableComms
from common.messages import Message, End, Start
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config


class AggregatorComms(
    Generic[GenericJoinedTrip, GenericAggregatedRecord],
    ReliableComms[
        Message[GenericJoinedTrip | End | Start], Message[GenericAggregatedRecord | End]
    ],
):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        # in
        others_queue = self.config.in_others_queue_format.format(host_id=self.id)
        self.channel.queue_declare(others_queue)  # end
        for rk in self.config.in_others_queue_routing_keys:
            self.channel.queue_bind(others_queue, self.config.in_exchange, rk)
        self._start_consuming_from(others_queue)

    def _get_routing_details(
        self, msg: Message[GenericAggregatedRecord | End]
    ) -> tuple[str, str]:
        return self.config.out_exchange, self.config.out_queue

    def start_consuming_trips(self, job_id: str) -> None:
        self._start_consuming_from(self.__trips_queue(job_id))

    def stop_consuming_trips(self, job_id: str) -> None:
        self._stop_consuming_from(self.__trips_queue(job_id))

    def set_all_trips_done_callback(
        self, job_id: str, callback: Callable[[], None]
    ) -> None:
        self._set_empty_queue_callback(self.__trips_queue(job_id), callback)

    def __trips_queue(self, job_id: str) -> str:
        return self.config.in_trips_queue_format.format(job_id=job_id)
