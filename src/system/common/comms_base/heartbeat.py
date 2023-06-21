from dataclasses import dataclass

from shared.serde import serialize
from . import SystemCommunicationBase
from ..config_base import ConfigBase


@dataclass
class AliveMessage:
    container_name: str


class HeartbeatSender:
    comms: SystemCommunicationBase
    config: ConfigBase

    def __init__(self, comms: SystemCommunicationBase, config: ConfigBase) -> None:
        self.comms = comms
        self.config = config

    def setup_timer(self) -> None:
        self.comms.connection.call_later(
            self.config.heartbeat_frequency, self.__heartbeat_timer
        )

    def __heartbeat_timer(self) -> None:
        self.send_heartbeat()
        self.setup_timer()

    def send_heartbeat(self) -> None:
        self.comms.channel.basic_publish(
            self.config.heartbeat_exchange,
            self.config.heartbeat_routing_key,
            serialize(AliveMessage(self.comms.name)).encode(),
        )
