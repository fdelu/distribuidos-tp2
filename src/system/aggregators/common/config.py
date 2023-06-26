from common.config_base import ConfigBase


class Config(ConfigBase):
    name: str
    joiners_count: int
    send_interval_seconds: float
    prefetch_count: int

    in_exchange: str
    in_trips_queue_format: str
    in_others_queue_format: str
    in_others_queue_routing_keys: list[str]
    out_exchange: str
    out_queue: str

    host_count: int
    filters_exchange: str
    filters_routing_keys_format: list[str]
    filters_queue_format: str

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(ConfigBase.subsection("aggregators", name))
        self.send_interval_seconds = self.get_float("SendIntervalSeconds")
        self.joiners_count = self.get_int(f"{name.upper()}_JOINERS_SCALE")
        self.prefetch_count = self.get_int("PrefetchCount")

        self.in_exchange = self.get_named("InExchangeFormat")
        self.in_trips_queue_format = self.get_named("InTripsQueueFormat")
        self.in_others_queue_format = self.get_named("InOthersQueueFormat")
        self.in_others_queue_routing_keys = self.get_json("InOthersQueueRoutingKeys")
        self.out_exchange = self.get("InExchange", section="reducers")
        self.out_queue = self.get("InQueueFormat", section="reducers").replace(
            "{name}", self.name
        )
        self.host_count = self.get_int(f"{name.upper()}_AGGREGATORS_SCALE")
        self.filters_exchange = self.get_named("FiltersExchangeFormat")
        self.filters_routing_keys_format = self.get_json("FiltersRoutingKeysFormat")
        self.filters_queue_format = self.get_named("FiltersQueueFormat")

    def get_named(self, key: str) -> str:
        return super().get(key).replace("{name}", self.name)
