from typing import Protocol, TypeVar, Generic
from enum import StrEnum
from dataclasses import dataclass
from shared.messages import RecordType as BaseRecordType

T = TypeVar("T", covariant=True)


class RecordType(StrEnum):
    TRIP = BaseRecordType.TRIP
    STATION = BaseRecordType.STATION
    WEATHER = BaseRecordType.WEATHER
    END = "end"
    RAW_BATCH = "raw_batch"
    TRIPS_START = "trips_start"


class WithRoutingKey(Protocol):
    def get_routing_key(self) -> str:
        ...


@dataclass
class Batch(Generic[T]):
    messages: list[T]
    msg_id: str | None


P = TypeVar("P", covariant=True, bound=WithRoutingKey)


@dataclass
class Message(Generic[P]):
    job_id: str
    payload: P

    def get_routing_key(self) -> str:
        return f"{self.job_id}.{self.payload.get_routing_key()}"


class End:
    def get_routing_key(self) -> str:
        return RecordType.END

    def be_handled_by(self, handler: "EndHandler[T]") -> T:
        return handler.handle_end()


class EndHandler(Protocol[T]):
    def handle_end(self) -> T:
        ...
