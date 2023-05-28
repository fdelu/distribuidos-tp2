from abc import abstractmethod
from typing import Protocol, Callable

from common.messages import End
from common.messages.basic import BasicRecord
from common.messages.joined import GenericJoinedTrip
from common.comms_base.protocol import CommsReceiveProtocol, CommsSendProtocol


class JoinerComms(
    CommsReceiveProtocol[BasicRecord],
    CommsSendProtocol[GenericJoinedTrip | End],
    Protocol[GenericJoinedTrip],
):
    @abstractmethod
    def set_all_trips_done_callback(self, callback: Callable[[], None]) -> None:
        ...

    @abstractmethod
    def start_consuming_trips(self) -> None:
        ...
