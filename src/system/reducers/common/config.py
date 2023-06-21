from common.config_base import ConfigBase


class Config(ConfigBase):
    aggregators_count: int
    prefetch_count: int
    name: str

    in_exchange: str
    in_queue: str
    out_exchange: str
    out_queue: str

    def __init__(self, name: str) -> None:
        super().__init__(f"reducers.{name}")
        self.aggregators_count = self.get_int("AggregatorsCount")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.name = name

        self.in_exchange = self.get("InExchange")
        self.in_queue = self.get("InQueue")
        self.out_exchange = self.get("OutExchange")
        self.out_queue = self.get("OutQueue")
