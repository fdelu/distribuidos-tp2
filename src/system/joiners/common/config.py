from common.config_base import ConfigBase

SECTION = "joiners"


class Config(ConfigBase):
    parsers_count: int
    prefetch_count: int

    in_exchange: str
    in_trips_queue_format: str
    in_others_queue_format: str
    in_others_queue_routing_keys: list[str]
    out_exchange: str
    out_queues: dict[str, list[str]]  # queue -> routing keys

    def __init__(self, name: str) -> None:
        super().__init__(ConfigBase.subsection(SECTION, name))
        self.parsers_count = self.get_int("ParsersCount")
        self.prefetch_count = self.get_int("PrefetchCount")

        self.in_exchange = self.get("InExchange")
        self.in_trips_queue_format = self.get("InTripsQueueFormat")
        self.in_others_queue_format = self.get("InOthersQueueFormat")
        self.in_others_queue_routing_keys = self.get_json("InOthersQueueRoutingKeys")
        self.out_exchange = self.get("OutExchange")
        self.out_queues = self.get_json("OutQueues")
