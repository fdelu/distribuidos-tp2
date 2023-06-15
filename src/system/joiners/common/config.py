from common.config_base import ConfigBase

SECTION = "joiners"


class Config(ConfigBase):
    parsers_count: int
    prefetch_count: int
    name: str

    in_exchange: str
    in_trips_queue_format: str
    in_other_queue_format: str
    in_other_queue_routing_keys: list[str]
    out_exchange: str

    def __init__(self, name: str) -> None:
        super().__init__(ConfigBase.subsection(SECTION, name))
        self.parsers_count = self.get_int("ParsersCount")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.name = name

        self.in_exchange = self.get("InExchange")
        self.in_trips_queue_format = self.get("InTripsQueueFormat")
        self.in_other_queue_format = self.get("InOtherQueueFormat")
        self.in_other_queue_routing_keys = self.get_json("InOtherRoutingKeys")
        self.out_exchange = self.get("OutExchange")
