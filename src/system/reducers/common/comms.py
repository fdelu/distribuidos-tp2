from typing import Protocol

from common.messages import Message
from common.comms_base.protocol import CommsReceiveProtocol, CommsSendProtocol, IN
from common.messages.stats import StatsRecord


class ReducerComms(
    CommsReceiveProtocol[Message[IN]],
    CommsSendProtocol[Message[StatsRecord]],
    Protocol[IN],
):
    pass
