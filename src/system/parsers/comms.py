from typing import Callable

from common.comms_base import ReliableComms, setup_job_queues
from common.messages import Message
from common.messages.raw import RawRecord
from common.messages.basic import BasicRecord

from .config import Config


class SystemCommunication(
    ReliableComms[Message[RawRecord], Message[BasicRecord]],
):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config, duplicate_filter_config=config)

    def _load_definitions(self) -> None:
        # in
        for i in range(self.config.host_count):
            others_queue = self.config.in_others_queue_format.format(host_id=i)
            self.channel.queue_declare(others_queue)
            for rk in self.config.in_others_queue_routing_keys:
                self.channel.queue_bind(others_queue, self.config.in_exchange, rk)
        others_queue = self.config.in_others_queue_format.format(host_id=self.id)
        self._start_consuming_from(others_queue)

    def _get_routing_details(self, msg: Message[BasicRecord]) -> tuple[str, str]:
        return self.config.out_exchange, msg.get_routing_key()

    def set_all_batchs_done_callback(
        self, job_id: str, callback: Callable[[], None]
    ) -> None:
        self._set_empty_queue_callback(self.__batchs_queue(job_id), callback)

    def start_consuming_job(self, job_id: str) -> None:
        queue = self.__batchs_queue(job_id)
        self._start_consuming_from(queue)
        setup_job_queues(self, self.config.out_exchange, self.config.out_queues, job_id)

    def stop_consuming_job(self, job_id: str) -> None:
        self._stop_consuming_from(self.__batchs_queue(job_id))

    def __batchs_queue(self, job_id: str) -> str:
        return self.config.in_batchs_queue_format.format(job_id=job_id)
