from common.config_base import ConfigBase


class Config(ConfigBase):
    prefetch_count: int

    # Middleware settings
    in_exchange: str
    in_batchs_queue_format: str
    in_others_queue_format: str
    in_others_queue_routing_keys: list[str]
    out_exchange: str
    out_queues: dict[str, list[str]]  # queue -> routing keys

    def __init__(self) -> None:
        super().__init__("parsers")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.in_exchange = self.get("InExchange")
        self.in_batchs_queue_format = self.get("InBatchsQueueFormat")
        self.in_others_queue_format = self.get("InOthersQueueFormat")
        self.in_others_queue_routing_keys = self.get_json("InOthersQueueRoutingKeys")
        self.out_exchange = self.get("OutExchange")
        self.out_queues = self.get_json("OutQueues")
