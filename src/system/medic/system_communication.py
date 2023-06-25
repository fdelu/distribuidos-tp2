import logging
from .config import Config
from common.comms_base import SystemCommunicationBase, CommsReceive, CommsSend
from .messages.bully_messages import (
    BullyMessage,
    ElectionMessage,
    CoordinatorMessage,
    AnswerMessage,
    AliveMessage,
    AliveLeaderMessage,
)


class SystemCommunication(
    CommsReceive[BullyMessage], CommsSend[BullyMessage], SystemCommunicationBase
):
    exchange: str
    bully_queue: str
    config: Config
    medic_scale: int

    def __init__(self, config: Config) -> None:
        self.bully_queue = f"bully_{self.id}"
        self.medic_scale = config.medic_scale
        self.exchange = config.medic_exchange
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        self.channel.queue_declare(queue=self.bully_queue)
        self.channel.queue_bind(self.bully_queue, self.exchange, f"#.{self.id}.#")

    def bind_heartbeat_route(self) -> None:
        self._start_consuming_from(self.config.heartbeat_routing_key)

    def unbind_heartbeat_route(self) -> None:
        self._stop_consuming_from(
            self.config.heartbeat_routing_key, delete_if_unused=False
        )

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
            logging.debug(f"Election message to route: {routing_key}")
        elif isinstance(record, CoordinatorMessage):
            routing_key = self.create_routing_key(1, self.medic_scale)
            logging.debug(f"Coordinator message to route: {routing_key}")
        elif isinstance(record, AnswerMessage):
            routing_key = self.create_routing_key(
                record.receiver_id, record.receiver_id
            )
            logging.debug(f"Answer message to route: {routing_key}")
        elif isinstance(record, AliveLeaderMessage):
            routing_key = self.create_routing_key(1, self.medic_scale)
            # logging.info(f"Alive leader message to route: {routing_key}")
        elif isinstance(record, AliveMessage):
            return self.config.heartbeat_exchange, self.config.heartbeat_routing_key
        return self.exchange, routing_key
