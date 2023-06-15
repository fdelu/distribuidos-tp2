from common.comms_base import SystemCommunicationBase, CommsSend, setup_job_queues
from common.messages import Package, Message
from common.messages.raw import RawRecord

from .config import Config


class SystemCommunication(
    CommsSend[Package[Message[RawRecord]]], SystemCommunicationBase
):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _get_routing_details(self, msg: Package[Message[RawRecord]]) -> tuple[str, str]:
        return self.config.out_exchange, msg.messages[0].get_routing_key()

    def setup_job_queue(self, job_id: str) -> None:
        setup_job_queues(
            self, self.config.out_exchange, self.config.out_batchs_queues, job_id
        )
