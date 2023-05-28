from common.config_base import ConfigBase


class Config(ConfigBase):
    aggregators_count: int
    prefetch_count: int

    def __init__(self, name: str):
        super().__init__(f"reducers.{name}")
        self.aggregators_count = self.get_int("AggregatorsCount")
        self.prefetch_count = self.get_int("PrefetchCount")
