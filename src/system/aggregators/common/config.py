from common.config_base import ConfigBase


class Config(ConfigBase):
    joiners_count: int
    send_interval_seconds: float
    prefetch_count: int
    name: str

    in_exchange: str
    in_trips_queue_format: str
    in_others_queue_format: str
    in_others_queue_routing_keys: list[str]
    out_exchange: str
    out_queue: str

    host_count: int
    filters_exchange: str
    filters_routing_keys_format: list[str]

    def __init__(self, name: str) -> None:
        super().__init__(f"aggregators.{name}")
        self.send_interval_seconds = self.get_float("SendIntervalSeconds")
        self.joiners_count = self.get_int(f"{name.upper()}_JOINERS_SCALE")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.name = name

        self.in_exchange = self.get("InExchange")
        self.in_trips_queue_format = self.get("InTripsQueueFormat")
        self.in_others_queue_format = self.get("InOthersQueueFormat")
        self.in_others_queue_routing_keys = self.get_json("InOthersQueueRoutingKeys")
        self.out_exchange = self.get("OutExchange")
        self.out_queue = self.get("OutQueue")
        self.host_count = self.get_int(f"{name.upper()}_AGGREGATORS_SCALE")
        self.filters_exchange = self.get("FiltersExchange")
        self.filters_routing_keys_format = self.get_json("FiltersRoutingKeysFormat")
