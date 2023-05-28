from typing import Protocol, TypeVar
from dataclasses import dataclass

from common.messages import End, RecordType

T = TypeVar("T", covariant=True)
U = TypeVar("U", contravariant=True)


@dataclass
class StationInfo:
    name: str
    latitude: float | None
    longitude: float | None


@dataclass()
class JoinedRainTrip:
    start_date: str
    duration_sec: float

    def be_handled_by(self, handler: "JoinedRecordHandler[T, JoinedRainTrip]") -> T:
        return handler.handle_joined(self)

    def get_routing_key(self) -> str:
        return RecordType.TRIP


JoinedRainRecords = JoinedRainTrip | End


@dataclass
class JoinedYearTrip:
    start_station_name: str
    year: str

    def be_handled_by(self, handler: "JoinedRecordHandler[T, JoinedYearTrip]") -> T:
        return handler.handle_joined(self)

    def get_routing_key(self) -> str:
        return RecordType.TRIP


JoinedYearRecords = JoinedYearTrip | End


@dataclass
class JoinedCityTrip:
    end_station_name: str
    start_station_coordinates: tuple[float, float]
    end_station_coordinates: tuple[float, float]

    def be_handled_by(self, handler: "JoinedRecordHandler[T, JoinedCityTrip]") -> T:
        return handler.handle_joined(self)

    def get_routing_key(self) -> str:
        return RecordType.TRIP


JoinedCityRecords = JoinedCityTrip | End


class JoinedRecordHandler(Protocol[T, U]):
    def handle_joined(self, trip: U) -> T:
        ...


JoinedRecord = JoinedRainTrip | JoinedYearTrip | JoinedCityTrip | End
GenericJoinedTrip = TypeVar(
    "GenericJoinedTrip",
    JoinedRainTrip,
    JoinedYearTrip,
    JoinedCityTrip,
    contravariant=True,
)
