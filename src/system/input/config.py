from common.config_base import ConfigBase
from common.util import singleton


@singleton
class Config(ConfigBase):
    address: str
    max_jobs: int

    # Middleware settings
    out_exchange: str
    out_batchs_queues: dict[str, list[str]]  # queue -> routing keys

    def __init__(self) -> None:
        super().__init__("input")
        self.address = self.get("Address")
        self.out_exchange = self.get("OutExchange")
        self.out_batchs_queues = self.get_json("OutBatchsQueues")
        self.max_jobs = self.get_int("MaxJobs")
