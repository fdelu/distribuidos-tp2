from common.config_base import ConfigBase
from common.util import singleton


@singleton
class Config(ConfigBase):
    address: str
    prefetch_count: int
    in_exchange: str
    in_queue: str

    def __init__(self) -> None:
        super().__init__("output")
        self.address = self.get("Address")
        self.prefetch_count = self.get_int("PrefetchCount")

        self.in_exchange = self.get("InExchange")
        self.in_queue = self.get("InQueue")
