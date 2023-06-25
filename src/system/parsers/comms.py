from typing import Callable

from common.comms_base import ReliableComms, setup_job_queues, HeartbeatSender
from common.messages.raw import RawRecord
from common.messages.basic import BasicRecord

from .config import Config


class SystemCommunication(ReliableComms[RawRecord, BasicRecord]):
    def __init__(self) -> None:
        super().__init__(Config(), duplicate_filter_config=Config())
        HeartbeatSender(self, Config()).setup_timer()

    def _load_definitions(self) -> None:
        # in
        for id in range(1, Config().host_count + 1):
            others_queue = Config().in_others_queue_format.format(host_id=id)
            self.channel.queue_declare(others_queue)
            for rk in Config().in_others_queue_routing_keys:
                self.channel.queue_bind(others_queue, Config().in_exchange, rk)
        others_queue = Config().in_others_queue_format.format(host_id=self.id)
        self._start_consuming_from(others_queue)

    def _get_routing_details(self, msg: BasicRecord) -> tuple[str, str]:
        return Config().out_exchange, msg.get_routing_key()

    def set_all_weather_station_lines_done_callback(
        self, job_id: str, callback: Callable[[], None]
    ) -> None:
        self._set_empty_queue_callback(
            self.__weather_station_lines_queue(job_id), callback
        )

    def set_all_trip_lines_done_callback(
        self, job_id: str, callback: Callable[[], None]
    ) -> None:
        self._set_empty_queue_callback(self.__trip_lines_queue(job_id), callback)

    def start_consuming_weather_station_lines(self, job_id: str) -> None:
        queue = self.__weather_station_lines_queue(job_id)
        self._start_consuming_from(queue)
        setup_job_queues(self, Config().out_exchange, Config().out_queues, job_id)

    def start_consuming_trip_lines(self, job_id: str) -> None:
        queue = self.__trip_lines_queue(job_id)
        self._start_consuming_from(queue)

    def stop_consuming_weather_station_lines(self, job_id: str) -> None:
        self._stop_consuming_from(self.__weather_station_lines_queue(job_id))

    def stop_consuming_trip_lines(self, job_id: str) -> None:
        self._stop_consuming_from(self.__trip_lines_queue(job_id))

    def __weather_station_lines_queue(self, job_id: str) -> str:
        return Config().in_weather_station_lines_format.format(job_id=job_id)

    def __trip_lines_queue(self, job_id: str) -> str:
        return Config().in_trip_lines_format.format(job_id=job_id)
