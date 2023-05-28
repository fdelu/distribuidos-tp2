from common.comms_base import SystemCommunicationBase, CommsSend
from common.messages.raw import RawRecord


class SystemCommunication(CommsSend[RawRecord], SystemCommunicationBase):
    OUT_EXCHANGE = "raw_records"

    def _get_routing_details(self, record: RawRecord) -> tuple[str, str]:
        return self.OUT_EXCHANGE, record.get_routing_key()
