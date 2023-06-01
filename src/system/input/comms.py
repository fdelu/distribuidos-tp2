from common.comms_base import SystemCommunicationBase, CommsSend
from common.messages import Message
from common.messages.raw import RawRecord


class SystemCommunication(CommsSend[RawRecord], SystemCommunicationBase):
    OUT_EXCHANGE = "raw_records"

    def _get_routing_details(self, msg: Message[RawRecord]) -> tuple[str, str]:
        return self.OUT_EXCHANGE, msg.payload.get_routing_key()
