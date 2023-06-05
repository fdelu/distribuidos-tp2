from common.config_base import ConfigBase


class Config(ConfigBase):
    parsers_count: int
    prefetch_count: int
    batch_size: int
    name: str

    def __init__(self, name: str) -> None:
        super().__init__(f"joiners.{name}")
        self.parsers_count = self.get_int("ParsersCount")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.batch_size = self.get_int("BatchSize")
        self.name = name
