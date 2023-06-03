from typing import Protocol, TypeVar
from dataclasses import dataclass

from common.messages import End, RecordType

T = TypeVar("T", covariant=True)


@dataclass
class RawLines:
    record_type: RecordType
    city: str
    headers: str
    lines: list[str]

    def get_routing_key(self) -> str:
        return RecordType.RAW_BATCH

    def be_handled_by(self, handler: "RawRecordHandler[T]") -> T:
        if self.record_type == RecordType.WEATHER:
            return handler.handle_weather_batch(self)
        elif self.record_type == RecordType.STATION:
            return handler.handle_station_batch(self)
        elif self.record_type == RecordType.TRIP:
            return handler.handle_trip_batch(self)
        else:
            raise ValueError(f"Unknown record type: {self.record_type}")


class RawRecordHandler(Protocol[T]):
    def handle_weather_batch(self, batch: RawLines) -> T:
        ...

    def handle_station_batch(self, batch: RawLines) -> T:
        ...

    def handle_trip_batch(self, batch: RawLines) -> T:
        ...


RawRecord = RawLines | End
