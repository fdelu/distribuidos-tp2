from common.config_base import ConfigBase
from common.util import singleton


@singleton
class Config(ConfigBase):
    prefetch_count: int

    in_exchange: str
    in_trip_lines_format: str
    in_weather_station_lines_format: str
    in_others_queue_format: str
    in_others_queue_routing_keys: list[str]
    out_exchange: str
    out_queues_format: dict[str, list[str]]  # queue -> routing keys

    host_count: int
    filters_exchange: str
    filters_routing_keys_format: list[str]
    filters_queue_format: str

    def __init__(self) -> None:
        super().__init__("parsers")
        self.prefetch_count = self.get_int("PrefetchCount")
        self.in_exchange = self.get("InExchange")
        self.in_trip_lines_format = self.get("InTripLinesQueueFormat")
        self.in_weather_station_lines_format = self.get(
            "InWeatherStationLinesQueueFormat"
        )
        self.in_others_queue_format = self.get("InOthersQueueFormat")
        self.in_others_queue_routing_keys = self.get_json("InOthersQueueRoutingKeys")
        self.out_exchange = self.get("InExchange", section="joiners")
        self.host_count = self.get_int("PARSERS_SCALE")
        self.filters_exchange = self.get("FiltersExchange")
        self.filters_routing_keys_format = self.get_json("FiltersRoutingKeysFormat")
        self.filters_queue_format = self.get("FiltersQueueFormat")
        self.out_queues_format = self.__get_out_queues("joiners")

    def __get_out_queues(self, next_section: str) -> dict[str, list[str]]:
        out_queues = {}
        out_queues_format = self.get("InTripsQueueFormat", section=next_section)
        for pipeline in self.get_subsections(next_section):
            queue = out_queues_format.replace("{name}", pipeline)
            rks = [
                rk.replace("{name}", pipeline)
                for rk in self.get_json(
                    "InTripsQueueRoutingKeysFormat",
                    section=ConfigBase.subsection(next_section, pipeline),
                )
            ]
            out_queues[queue] = rks
        return out_queues
