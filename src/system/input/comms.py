from common.comms_base import SystemCommunicationBase, CommsSend, setup_job_queues
from common.messages import Message
from common.messages.comms import Package
from common.messages.raw import RawRecord
from common.persistence import StatePersistor

from .config import Config

PENDING_KEY = "_pending"
Output = Package[Message[RawRecord]]


class SystemCommunication(CommsSend[Output], SystemCommunicationBase):
    config: Config
    pending_package: Output | None = None

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)
        pending_package = StatePersistor().load(PENDING_KEY, Output)
        if pending_package is not None:
            self.send(pending_package)

    def _get_routing_details(self, msg: Output) -> tuple[str, str]:
        return self.config.out_exchange, msg.messages[0].get_routing_key()

    def setup_job_queue(self, job_id: str) -> None:
        setup_job_queues(
            self, self.config.out_exchange, self.config.out_batchs_queues, job_id
        )

    def send_msg(
        self, job_id: str, record: RawRecord, msg_id: str | None = None
    ) -> None:
        package = Package([Message(job_id, record)], msg_id)
        self.pending_package = package

    def flush(self) -> None:
        StatePersistor().store(PENDING_KEY, self.pending_package)
        StatePersistor().save()
        if self.pending_package is not None:
            self.send(self.pending_package)
        self.pending_package = None
