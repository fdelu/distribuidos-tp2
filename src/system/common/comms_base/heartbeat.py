from dataclasses import dataclass

from shared.serde import serialize

from .base import SystemCommunicationBase
from ..config_base import ConfigBase


@dataclass
class AliveMessage:
    container_name: str


class HeartbeatSender:
    comms: SystemCommunicationBase
    config: ConfigBase
    msg: bytes

    def __init__(self, comms: SystemCommunicationBase, config: ConfigBase) -> None:
        self.comms = comms
        self.config = config
        self.msg = serialize(AliveMessage(self.comms.name)).encode()

    def setup_timer(self) -> None:
        self.comms.connection.call_later(
            self.config.heartbeat_frequency, self.__heartbeat_timer
        )

    def __heartbeat_timer(self) -> None:
        self.setup_timer()
        self.send_heartbeat()

    def send_heartbeat(self) -> None:
        self.comms.channel.basic_publish(
            self.config.heartbeat_exchange,
            self.config.heartbeat_routing_key,
            self.msg,
        )
