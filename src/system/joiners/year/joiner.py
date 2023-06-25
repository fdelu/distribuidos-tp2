import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedYearTrip
from common.persistence import WithStateAppended

# (city, code, year) -> name
Key = tuple[str, str, str]
Value = str


class YearJoiner(WithStateAppended[Key, Value]):
    def handle_station(self, station: BasicStation) -> None:
        self.set((station.city, station.code, station.year), station.name)

    def handle_weather(self, weather: BasicWeather) -> None:
        logging.warn("Unexpected Weather received on year joiner")

    def handle_trip(self, trip: BasicTrip) -> JoinedYearTrip | None:
        name = self._get_join_data(trip)
        if name is None:
            return None

        return JoinedYearTrip(name, trip.year)

    def _get_join_data(self, trip: BasicTrip) -> str | None:
        name = self.state.get((trip.city, trip.start_station_code, trip.year), None)
        if name is None:
            logging.debug(
                f"Missing station name for code {trip.start_station_code}, year"
                f" {trip.year} ({trip.city})"
            )
            return None
        return name
