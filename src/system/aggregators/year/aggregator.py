import logging
from dataclasses import dataclass

from common.messages.joined import JoinedYearTrip
from common.messages.aggregated import PartialYearCounts
from common.persistence import WithState

from .config import Config


@dataclass
class Counts:
    # station name -> count
    counts_year_base: dict[str, int]
    counts_year_compared: dict[str, int]


class YearAggregator(WithState[Counts]):
    def __init__(self) -> None:
        super().__init__(Counts({}, {}))

    def handle_joined(self, trip: JoinedYearTrip) -> None:
        if trip.year == Config().year_base:
            self.__add_to(self.state.counts_year_base, trip)
        elif trip.year == Config().year_compared:
            self.__add_to(self.state.counts_year_compared, trip)
        else:
            logging.warning(f"Received trip from unexpected year: {trip.year}")

    def __add_to(self, counts: dict[str, int], trip: JoinedYearTrip) -> None:
        counts[trip.start_station_name] = counts.get(trip.start_station_name, 0) + 1

    def get_value(self) -> PartialYearCounts | None:
        if len(self.state.counts_year_base) + len(self.state.counts_year_compared) == 0:
            return None
        return PartialYearCounts(
            self.state.counts_year_base, self.state.counts_year_compared
        )

    def reset(self) -> None:
        self.state = Counts({}, {})
