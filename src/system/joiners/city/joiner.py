from dataclasses import dataclass
import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedCityTrip
from common.persistence import WithState


@dataclass
class StationData:
    name: str
    coordinates: tuple[float, float]


# city -> (code, year) -> data
Stations = dict[str, dict[tuple[str, str], StationData]]


class CityJoiner(WithState[Stations]):
    def __init__(self) -> None:
        super().__init__({})

    def handle_station(self, station: BasicStation) -> None:
        if station.latitude is None or station.longitude is None:
            logging.debug(
                f"Missing coordinates for station code {station.code}, year"
                f" {station.year}"
            )
            return
        station_names = self.state.setdefault(station.city, {})
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
        station = self.state[city].get((code, year), None)
        if station is None:
            logging.debug(f"Missing station data for code {code}, year {year} ({city})")
        return station
