from common.config_base import ConfigBase
from common.util import singleton


@singleton
class Config(ConfigBase):
    address: str
    prefetch_count: int
    medic_scale: int
    coordinator_timeout: int
    answer_timeout: int
    heartbeat_interval: int
    heartbeat_timeout: int
    leader_heartbeat_interval: int
    leader_heartbeat_timeout: int
    first_heartbeat_timeout: int
    restart_timeout: int
    medic_exchange: str
    medic_queue_format: str
    medic_queue_routing_keys_format: list[str]

    def __init__(self) -> None:
        super().__init__("medic")
        self.medic_scale = self.get_int("MEDIC_SCALE")
        self.prefetch_count = self.get_int("PrefetchCount")

        self.coordinator_timeout = self.get_int("CoordinatorTimeout")
        self.answer_timeout = self.get_int("AnswerTimeout")
        self.heartbeat_interval = self.get_int("HeartbeatInterval")
        self.heartbeat_timeout = self.get_int("HeartbeatTimeout")
        self.leader_heartbeat_interval = self.get_int("LeaderHeartbeatInterval")
        self.leader_heartbeat_timeout = self.get_int("LeaderHeartbeatTimeout")
        self.first_heartbeat_timeout = self.get_int("FirstHeartbeatTimeout")
        self.restart_timeout = self.get_int("RestartTimeout")
        self.medic_exchange = self.get("MedicExchange")
        self.medic_queue_format = self.get("MedicQueueFormat")
        self.medic_queue_routing_keys_format = self.get_json(
            "MedicQueueRoutingKeysFormat"
        )
