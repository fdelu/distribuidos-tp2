from common.comms_base import SystemCommunicationBase, CommsSend
from common.messages import Batch, Message
from common.messages.raw import RawRecord


class SystemCommunication(
    CommsSend[Batch[Message[RawRecord]]], SystemCommunicationBase
):
    OUT_EXCHANGE = "raw_records"

    def _get_routing_details(self, msg: Batch[Message[RawRecord]]) -> tuple[str, str]:
        return self.OUT_EXCHANGE, msg.messages[0].payload.get_routing_key()
