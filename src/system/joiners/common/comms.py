from typing import Callable, Generic

from common.messages import End, Start
from common.messages.basic import BasicRecord
from common.messages.joined import GenericJoinedTrip
from common.comms_base import ReliableComms, setup_job_queues

from .config import Config

__all__ = ["GenericJoinedTrip"]


class JoinerComms(
    Generic[GenericJoinedTrip],
    ReliableComms[BasicRecord, GenericJoinedTrip | End | Start],
):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config, duplicate_filter_config=config)

    def _load_definitions(self) -> None:
        # in
        for id in range(1, self.config.host_count + 1):
            others_queue = self.config.in_others_queue_format.format(host_id=id)
            self.channel.queue_declare(others_queue)  # for station, tripstart & end
            for rk in self.config.in_others_queue_routing_keys:
                self.channel.queue_bind(others_queue, self.config.in_exchange, rk)

        others_queue = self.config.in_others_queue_format.format(host_id=self.id)
        self._start_consuming_from(others_queue)

    def _get_routing_details(
        self, msg: GenericJoinedTrip | End | Start
    ) -> tuple[str, str]:
        return self.config.out_exchange, msg.get_routing_key()

    def start_consuming_trips(self, job_id: str) -> None:
        self._start_consuming_from(self.__trips_queue(job_id))

        setup_job_queues(self, self.config.out_exchange, self.config.out_queues, job_id)

    def stop_consuming_trips(self, job_id: str) -> None:
        self._stop_consuming_from(self.__trips_queue(job_id))

    def set_all_trips_done_callback(
        self, job_id: str, callback: Callable[[], None]
    ) -> None:
        self._set_empty_queue_callback(self.__trips_queue(job_id), callback)

    def __trips_queue(self, job_id: str) -> str:
        return self.config.in_trips_queue_format.format(job_id=job_id)
