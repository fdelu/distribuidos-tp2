from common.comms_base import SystemCommunicationBase, CommsReceive, CommsSend
from .config import Config
from .messages.bully_messages import (
    BullyMessage,
    ElectionMessage,
    CoordinatorMessage,
    AnswerMessage,
    AliveMessage,
    AliveLeaderMessage,
)
import logging


class SystemCommunication(
    CommsReceive[BullyMessage], CommsSend[BullyMessage], SystemCommunicationBase
):
    EXCHANGE = "medics"
    bully_queue: str
    config: Config
    medic_scale: int

    def __init__(self, config: Config) -> None:
        self.bully_queue = f"bully_{self.id}"
        self.medic_scale = config.medic_scale
        super().__init__(config)

    def _load_definitions(self) -> None:
        self.channel.queue_declare(queue=self.bully_queue, exclusive=True)
        self.channel.queue_bind(self.bully_queue, self.EXCHANGE, f"#.{self.id}.#")

    def bind_heartbeat_route(self) -> None:
        self.channel.queue_bind(self.bully_queue, self.EXCHANGE, "heartbeat")

    def unbind_heartbeat_route(self) -> None:
        self.channel.queue_unbind(self.bully_queue, self.EXCHANGE, "heartbeat")

    def create_routing_key(self, start: int, end: int) -> str:
        routing_key = ""
        for i in range(start, end + 1):
            if i == int(self.id):
                continue
            routing_key += str(i) + "."
        return routing_key[:-1]

    def _get_routing_details(self, record: BullyMessage) -> tuple[str, str]:
        routing_key = self.create_routing_key(1, self.medic_scale)
        if isinstance(record, ElectionMessage):
            routing_key = self.create_routing_key(int(self.id) + 1, self.medic_scale)
            logging.info(f"Election message to route: {routing_key}")
        elif isinstance(record, CoordinatorMessage):
            routing_key = self.create_routing_key(1, self.medic_scale)
            logging.info(f"Coordinator message to route: {routing_key}")
        elif isinstance(record, AnswerMessage):
            routing_key = self.create_routing_key(
                record.receiver_id, record.receiver_id)
            logging.info(f"Awnser message to route: {routing_key}")
        elif isinstance(record, AliveLeaderMessage):
            routing_key = self.create_routing_key(1, self.medic_scale)
            # logging.info(f"Alive leader message to route: {routing_key}")
        elif isinstance(record, AliveMessage):
            routing_key = "heartbeat"
            logging.debug(f"Alive message to route: {routing_key}")
        return self.EXCHANGE, routing_key
