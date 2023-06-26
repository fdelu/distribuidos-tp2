from common.config_base import ConfigBase


class Config(ConfigBase):
    name: str
    aggregators_count: int
    prefetch_count: int

    in_exchange: str
    in_queue: str
    out_exchange: str
    out_queue: str

    def __init__(self, name: str) -> None:
        super().__init__(f"reducers.{name}")
        self.aggregators_count = self.get_int(f"{name.upper()}_AGGREGATORS_SCALE")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.name = name

        self.in_exchange = self.get("InExchange")
        self.in_queue = self.get("InQueueFormat").replace("{name}", self.name)
        self.out_exchange = self.get("InExchange", section="output")
        self.out_queue = self.get("InQueue", section="output")
