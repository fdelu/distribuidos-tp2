import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedYearTrip

from .config import Config


class YearJoiner:
    # city -> (code, year) -> name
    station_names: dict[str, dict[tuple[str, str], str]]
    config: Config

    def __init__(self, config: Config) -> None:
        self.station_names = {}
        self.config = config

    def handle_station(self, station: BasicStation) -> None:
        station_names = self.station_names.setdefault(station.city, {})
        station_names[(station.code, station.year)] = station.name

    def handle_weather(self, weather: BasicWeather) -> None:
        logging.warn("Unexpected Weather received on year joiner")

    def handle_trip(self, trip: BasicTrip) -> JoinedYearTrip | None:
        name = self._get_join_data(trip)
        if name is None:
            return None

        return JoinedYearTrip(name, trip.year)

    def _get_join_data(self, trip: BasicTrip) -> str | None:
        name = self.station_names[trip.city].get(
            (trip.start_station_code, trip.year), None
        )
        if name is None:
            logging.debug(
                f"Missing station name for code {trip.start_station_code}, year"
                f" {trip.year} ({trip.city})"
            )
            return None
        return name
