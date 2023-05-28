from common.config_base import ConfigBase


class Config(ConfigBase):
    joiners_count: int
    send_interval_seconds: float
    prefetch_count: int

    def __init__(self, name: str) -> None:
        super().__init__(f"aggregators.{name}")
        self.send_interval_seconds = self.get_float("SendIntervalSeconds")
        self.joiners_count = self.get_int("JoinersCount")
        self.prefetch_count = self.get_int("PrefetchCount")
