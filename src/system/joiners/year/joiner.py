import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedYearTrip

from ..common.comms import JoinerComms
from .config import Config


class YearJoiner:
    # city -> (code, year) -> name
    station_names: dict[str, dict[tuple[str, str], str]]
    config: Config
    comms: JoinerComms[JoinedYearTrip]

    def __init__(self, config: Config, comms: JoinerComms):
        self.station_names = {}
        self.config = config
        self.comms = comms

    def handle_station(self, station: BasicStation):
        station_names = self.station_names.setdefault(station.city, {})
        station_names[(station.code, station.year)] = station.name

    def handle_weather(self, weather: BasicWeather):
        logging.warn("Unexpected Weather received on year joiner")

    def handle_trip(self, trip: BasicTrip):
        name = self._get_join_data(trip)
        if name is None:
            return

        joined_trip = JoinedYearTrip(name, trip.year)
        self.comms.send(joined_trip)

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
