from common.config_base import ConfigBase


class Config(ConfigBase):
    address: str
    prefetch_count: int
    medic_scale: int
    coordinator_timeout: int
    awnser_timeout: int
    heartbeat_interval: int
    heartbeat_timeout: int
    leader_heartbeat_interval: int
    leader_heartbeat_timeout: int
    first_heartbeat_timeout: int
    restart_timeout: int

    def __init__(self) -> None:
        super().__init__("medic")
        self.medic_scale = self.get_int("MEDIC_SCALE")
        self.prefetch_count = self.get_int("PrefetchCount")

        self.coordinator_timeout = self.get_int("CoordinatorTimeout")
        self.awnser_timeout = self.get_int("AwnserTimeout")
        self.heartbeat_interval = self.get_int("HeartbeatInterval")
        self.heartbeat_timeout = self.get_int("HeartbeatTimeout")
        self.leader_heartbeat_interval = self.get_int("LeaderHeartbeatInterval")
        self.leader_heartbeat_timeout = self.get_int("LeaderHeartbeatTimeout")
        self.first_heartbeat_timeout = self.get_int("FirstHeartbeatTimeout")
        self.restart_timeout = self.get_int("RestartTimeout")
