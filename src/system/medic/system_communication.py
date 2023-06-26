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
from typing import Any


class SystemCommunication(
    CommsReceive[BullyMessage], CommsSend[BullyMessage], SystemCommunicationBase
):
    bully_queue: str

    def __init__(self) -> None:
        self.bully_queue = Config().medic_queue_format.format(host_id=self.id)
        super().__init__(Config())

    def _load_definitions(self) -> None:
        self.channel.queue_declare(queue=self.bully_queue)
        for rk in Config().medic_queue_routing_keys_format:
            self.channel.queue_bind(
                self.bully_queue, Config().medic_exchange, rk.format(host_id=self.id)
            )

    def start_consuming_clean(self, callback: Any) -> None:
        self.set_callback(callback)
        self.channel.queue_purge(self.bully_queue)
        self._start_consuming_from(self.bully_queue)

    def bind_heartbeat_route(self) -> None:
        self.channel.queue_purge(Config().heartbeat_routing_key)
        self._start_consuming_from(Config().heartbeat_routing_key)

    def unbind_heartbeat_route(self) -> None:
        self._stop_consuming_from(
            Config().heartbeat_routing_key, delete_if_unused=False
        )

    def create_routing_key(self, start: int, end: int) -> str:
        routing_key = ""
        for i in range(start, end + 1):
            if i == int(self.id):
                continue
            routing_key += str(i) + "."
        return routing_key[:-1]

    def _get_routing_details(self, record: BullyMessage) -> tuple[str, str]:
        routing_key = self.create_routing_key(1, Config().medic_scale)
        if isinstance(record, ElectionMessage):
            routing_key = self.create_routing_key(
                int(self.id) + 1, Config().medic_scale
            )
            logging.debug(f"Election message to route: {routing_key}")
        elif isinstance(record, CoordinatorMessage):
            routing_key = self.create_routing_key(1, Config().medic_scale)
            logging.debug(f"Coordinator message to route: {routing_key}")
        elif isinstance(record, AnswerMessage):
            routing_key = self.create_routing_key(
                record.receiver_id, record.receiver_id
            )
            logging.debug(f"Answer message to route: {routing_key}")
        elif isinstance(record, AliveLeaderMessage):
            routing_key = self.create_routing_key(1, Config().medic_scale)
            # logging.info(f"Alive leader message to route: {routing_key}")
        elif isinstance(record, AliveMessage):
            return Config().heartbeat_exchange, Config().heartbeat_routing_key
        return Config().medic_exchange, routing_key
