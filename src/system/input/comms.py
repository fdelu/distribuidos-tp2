from common.comms_base import SystemCommunicationBase, CommsSend, setup_job_queues
from common.messages.comms import Package
from common.messages.raw import RawRecord
from common.persistence import StatePersistor

from .config import Config

PENDING_KEY = "_pending"
Output = Package[RawRecord]


class SystemCommunication(CommsSend[Output], SystemCommunicationBase):
    pending_package: Output | None = None

    def __init__(self) -> None:
        super().__init__(Config())
        pending_package = StatePersistor().load(PENDING_KEY, Output)
        if pending_package is not None:
            self.send(pending_package)

    def _get_routing_details(self, msg: Output) -> tuple[str, str]:
        return (
            Config().out_exchange,
            f"{msg.job_id}.{msg.messages[0].get_routing_key()}",
        )

    def setup_job_queue(self, job_id: str) -> None:
        setup_job_queues(
            self, Config().out_exchange, Config().out_batchs_queues, job_id
        )

    def send_msg(
        self, job_id: str, record: RawRecord, msg_id: str | None = None
    ) -> None:
        package = Package([record], msg_id, job_id)
        self.pending_package = package

    def flush(self) -> None:
        StatePersistor().store(PENDING_KEY, self.pending_package)
        StatePersistor().save()
        if self.pending_package is not None:
            self.send(self.pending_package)
        self.pending_package = None
