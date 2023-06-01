from dataclasses import dataclass
import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedCityTrip

from .config import Config


@dataclass
class StationData:
    name: str
    coordinates: tuple[float, float]


class CityJoiner:
    # city -> (code, year) -> data
    station_names: dict[str, dict[tuple[str, str], StationData]]
    config: Config

    def __init__(self, config: Config) -> None:
        self.station_names = {}
        self.config = config

    def handle_station(self, station: BasicStation) -> None:
        if station.latitude is None or station.longitude is None:
            logging.debug(
                f"Missing coordinates for station code {station.code}, year"
                f" {station.year}"
            )
            return
        station_names = self.station_names.setdefault(station.city, {})
        station_names[(station.code, station.year)] = StationData(
            station.name, (station.latitude, station.longitude)
        )

    def handle_weather(self, weather: BasicWeather) -> None:
        logging.warn("Unexpected Weather received on year joiner")

    def handle_trip(self, trip: BasicTrip) -> JoinedCityTrip | None:
        data = self._get_join_data(trip)
        if data is None:
            return None
        start, end = data
        return JoinedCityTrip(end.name, start.coordinates, end.coordinates)

    def _get_join_data(self, trip: BasicTrip) -> tuple[StationData, StationData] | None:
        start = self.__get_station_data(trip.start_station_code, trip.year, trip.city)
        if start is None:
            return None
        end = self.__get_station_data(trip.end_station_code, trip.year, trip.city)
        if end is None:
            return None
        return start, end

    def __get_station_data(self, code: str, year: str, city: str) -> StationData | None:
        station = self.station_names[city].get((code, year), None)
        if station is None:
            logging.debug(f"Missing station data for code {code}, year {year} ({city})")
        return station
