import logging
from common.messages.joined import JoinedYearTrip
from common.messages.aggregated import PartialYearCounts

from .config import Config


class YearAggregator:
    counts_year_base: dict[str, int]  # station name -> count
    counts_year_compared: dict[str, int]  # station name -> count
    config: Config

    def __init__(self, config: Config):
        self.counts_year_base = {}
        self.counts_year_compared = {}
        self.config = config

    def handle_joined(self, trip: JoinedYearTrip):
        if trip.year == self.config.year_base:
            self.__add_to(self.counts_year_base, trip)
        elif trip.year == self.config.year_compared:
            self.__add_to(self.counts_year_compared, trip)
        else:
            logging.warning(f"Received trip from unexpected year: {trip.year}")

    def __add_to(self, counts: dict[str, int], trip: JoinedYearTrip):
        counts[trip.start_station_name] = counts.get(trip.start_station_name, 0) + 1

    def get_value(self) -> PartialYearCounts:
        return PartialYearCounts(self.counts_year_base, self.counts_year_compared)

    def reset(self):
        self.counts_year_base = {}
        self.counts_year_compared = {}
