from enum import StrEnum
from typing import Generic, TypeVar
from dataclasses import dataclass


class StatType(StrEnum):
    RAIN = "rain"
    YEAR = "year"
    CITY = "city"


class RecordType(StrEnum):
    STATION = "station"
    TRIP = "trip"
    WEATHER = "weather"


# Client Messages


@dataclass
class NewJob:
    identity: str


@dataclass
class RecordStart:
    record_type: RecordType
    city: str
    headers: str


@dataclass
class LinesBatch:
    batch_number: int
    lines: list[str]


@dataclass
class AllSent:
    ...


@dataclass
class GetStat:
    stat_type: StatType


ClientPayloadsInput = RecordStart | LinesBatch | AllSent
ClientPayloadsOutput = GetStat
ClientPayloads = ClientPayloadsInput | ClientPayloadsOutput
T = TypeVar("T", bound=ClientPayloads)


@dataclass
class Message(Generic[T]):
    job_id: str
    payload: T


# Server Messages


@dataclass
class Ack:
    batch_number: int | None = None


@dataclass
class RainAverages:
    data: dict[str, float]  # date -> average duration

    def stat_type(self) -> StatType:
        return StatType.RAIN


@dataclass
class YearCounts:
    # station -> (count year base, count year duplicated)
    data: dict[str, tuple[int, int]]

    def stat_type(self) -> StatType:
        return StatType.YEAR


@dataclass
class CityAverages:
    data: dict[str, float]  # station -> average distance

    def stat_type(self) -> StatType:
        return StatType.CITY


@dataclass
class NotAvailable:
    ...


@dataclass
class ClientJobId:
    job_id: str


@dataclass
class Error:
    msg: str


Stat = RainAverages | YearCounts | CityAverages
ServerMessagesInput = Ack | Error | ClientJobId | NotAvailable
ServerMessagesOutput = NotAvailable | Stat
ServerMessages = ServerMessagesInput | ServerMessagesOutput
