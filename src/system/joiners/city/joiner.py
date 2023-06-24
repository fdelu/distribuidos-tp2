from dataclasses import dataclass
import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedCityTrip
from common.persistence import WithStateAppended


@dataclass
class StationData:
    name: str
    coordinates: tuple[float, float]


# (city, code, year) -> data
Key = tuple[str, str, str]
Value = StationData


class CityJoiner(WithStateAppended[Key, Value]):
    def handle_station(self, station: BasicStation) -> None:
        if station.latitude is None or station.longitude is None:
            logging.debug(
                f"Missing coordinates for station code {station.code}, year"
                f" {station.year}"
            )
            return
        self.set(
            (station.city, station.code, station.year),
            StationData(station.name, (station.latitude, station.longitude)),
        )

    def handle_weather(self, weather: BasicWeather) -> None:
        logging.warn("Unexpected Weather received on year joiner")

    def handle_trip(self, trip: BasicTrip) -> JoinedCityTrip | None:
        start = self.__get_station_data(trip.city, trip.start_station_code, trip.year)
        end = self.__get_station_data(trip.city, trip.end_station_code, trip.year)
        if start is None or end is None:
            return None
        return JoinedCityTrip(end.name, start.coordinates, end.coordinates)

    def __get_station_data(self, city: str, code: str, year: str) -> StationData | None:
        station = self.state.get((city, code, year), None)
        if station is None:
            logging.debug(f"Missing station data for code {code}, year {year} ({city})")
        return station
