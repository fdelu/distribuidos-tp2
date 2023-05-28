from typing import Protocol, TypeVar
from enum import StrEnum
from shared.messages import RecordType as BaseRecordType

T = TypeVar("T", covariant=True)


class RecordType(StrEnum):
    TRIP = BaseRecordType.TRIP
    STATION = BaseRecordType.STATION
    WEATHER = BaseRecordType.WEATHER
    END = BaseRecordType.END
    RAW_BATCH = "raw_batch"
    TRIPS_START = "trips_start"


class End:
    def get_routing_key(self) -> str:
        return BaseRecordType.END

    def be_handled_by(self, handler: "EndHandler[T]") -> T:
        return handler.handle_end()


class EndHandler(Protocol[T]):
    def handle_end(self) -> T:
        ...
