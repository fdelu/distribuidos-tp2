from common.comms_base import SystemCommunicationBase, CommsSend, setup_jobs_queues
from common.messages import Batch, Message
from common.messages.raw import RawRecord

from .config import Config


class SystemCommunication(
    CommsSend[Batch[Message[RawRecord]]], SystemCommunicationBase
):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _get_routing_details(self, msg: Batch[Message[RawRecord]]) -> tuple[str, str]:
        return self.config.out_exchange, msg.messages[0].get_routing_key()

    def setup_job_queue(self, job_id: str) -> None:
        setup_jobs_queues(
            self, self.config.out_exchange, self.config.out_batchs_queues, job_id
        )
