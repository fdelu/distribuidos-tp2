from common.config_base import ConfigBase


class Config(ConfigBase):
    name: str
    parsers_count: int
    prefetch_count: int

    in_exchange: str
    in_trips_queue_format: str
    in_others_queue_format: str
    in_others_queue_routing_keys: list[str]
    out_exchange: str
    out_queues: dict[str, list[str]]  # queue -> routing keys

    host_count: int
    filters_exchange: str
    filters_routing_keys_format: list[str]
    filters_queue_format: str

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(ConfigBase.subsection("joiners", name))
        self.parsers_count = self.get_int("PARSERS_SCALE")
        self.prefetch_count = self.get_int("PrefetchCount")

        self.in_exchange = self.get("InExchange")
        self.in_trips_queue_format = self.get_named("InTripsQueueFormat")
        self.in_others_queue_format = self.get_named("InOthersQueueFormat")
        self.in_others_queue_routing_keys = self.get_json("InOthersQueueRoutingKeys")
        self.out_exchange = self.get("InExchangeFormat", section="aggregators").replace(
            "{name}", self.name
        )
        self.host_count = self.get_int(f"{name.upper()}_JOINERS_SCALE")
        self.filters_exchange = self.get_named("FiltersExchangeFormat")
        self.filters_routing_keys_format = self.get_json("FiltersRoutingKeysFormat")
        self.filters_queue_format = self.get_named("FiltersQueueFormat")
        self.out_queues = self.__get_out_queues()

    def get_named(self, key: str) -> str:
        return super().get(key).replace("{name}", self.name)

    def __get_out_queues(self) -> dict[str, list[str]]:
        section = ConfigBase.subsection("aggregators", self.name)
        queue = self.get("InTripsQueueFormat", section=section)
        queue = queue.replace("{name}", self.name)
        rks = [
            rk.replace("{name}", self.name)
            for rk in self.get_json("InTripsQueueRoutingKeysFormat", section=section)
        ]
        return {queue: rks}
