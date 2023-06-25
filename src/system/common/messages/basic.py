from typing import Protocol, TypeVar
from dataclasses import dataclass

from common.messages import End, RecordType, TripsStart

T = TypeVar("T", covariant=True)


@dataclass()
class BasicStation:
    code: str
    name: str
    latitude: float | None
    longitude: float | None
    year: str
    city: str

    def get_routing_key(self) -> str:
        return f"{RecordType.STATION}.{self.city}.{self.year}"

    def be_handled_by(self, handler: "BasicStationHandler[T]") -> T:
        return handler.handle_station(self)


@dataclass()
class BasicTrip:
    start_date: str
    duration_sec: float
    city: str
    start_station_code: str
    end_station_code: str
    year: str

    def get_routing_key(self) -> str:
        return f"{RecordType.TRIP}.{self.city}.{self.year}"

    def be_handled_by(self, handler: "BasicTripHandler[T]") -> T:
        return handler.handle_trip(self)


@dataclass()
class BasicWeather:
    date: str
    precipitation: float
    city: str

    def get_routing_key(self) -> str:
        return f"{RecordType.WEATHER}.{self.city}"

    def be_handled_by(self, handler: "BasicWeatherHandler[T]") -> T:
        return handler.handle_weather(self)


BasicDataRecord = BasicStation | BasicTrip | BasicWeather
BasicControlRecord = TripsStart | End
BasicRecord = BasicDataRecord | BasicControlRecord


class BasicStationHandler(Protocol[T]):
    def handle_station(self, station: BasicStation) -> T:
        ...


class BasicWeatherHandler(Protocol[T]):
    def handle_weather(self, weather: BasicWeather) -> T:
        ...


class BasicTripHandler(Protocol[T]):
    def handle_trip(self, trip: BasicTrip) -> T:
        ...
