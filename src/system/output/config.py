from common.config_base import ConfigBase


class Config(ConfigBase):
    address: str
    prefetch_count: int

    def __init__(self) -> None:
        super().__init__("output")
        self.address = self.get("Address")
        self.prefetch_count = self.get_int("PrefetchCount")
