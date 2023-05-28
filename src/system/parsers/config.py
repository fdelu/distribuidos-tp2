from common.config_base import ConfigBase


class Config(ConfigBase):
    prefetch_count: int
    batch_size: int

    def __init__(self):
        super().__init__("parsers")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.batch_size = self.get_int("BatchSize")
