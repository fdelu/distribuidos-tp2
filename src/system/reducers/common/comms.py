from typing import Protocol

from common.comms_base.protocol import CommsReceiveProtocol, CommsSendProtocol, IN
from common.messages.stats import StatsRecord


class ReducerComms(
    CommsReceiveProtocol[IN], CommsSendProtocol[StatsRecord], Protocol[IN]
):
    pass
