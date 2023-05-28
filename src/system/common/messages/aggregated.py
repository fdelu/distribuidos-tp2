from typing import TypeVar, Protocol
from dataclasses import dataclass

from common.messages import End

T = TypeVar("T", covariant=True)
U = TypeVar("U", contravariant=True)


@dataclass
class DateInfo:
    count: int
    average_duration: float


@dataclass
class PartialRainAverages:
    duration_averages: dict[str, DateInfo]  # start_date -> DateInfo

    def be_handled_by(self, handler: "AggregatedHandler[T, PartialRainAverages]") -> T:
        return handler.handle_aggregated(self)


PartialRainRecords = PartialRainAverages | End


@dataclass
class PartialYearCounts:
    counts_year_base: dict[str, int]  # station name -> trip count
    counts_year_compared: dict[str, int]  # station name -> trip count

    def be_handled_by(self, handler: "AggregatedHandler[T, PartialYearCounts]") -> T:
        return handler.handle_aggregated(self)


PartialYearRecords = PartialYearCounts | End


@dataclass
class StationInfo:
    count: int
    average_distance: float


@dataclass
class PartialCityAverages:
    distance_averages: dict[str, StationInfo]  # station name -> StationInfo

    def be_handled_by(self, handler: "AggregatedHandler[T, PartialCityAverages]") -> T:
        return handler.handle_aggregated(self)


PartialCityRecords = PartialCityAverages | End


class AggregatedHandler(Protocol[T, U]):
    def handle_aggregated(self, aggregated: U) -> T:
        ...


GenericAggregatedRecord = TypeVar(
    "GenericAggregatedRecord",
    PartialRainAverages,
    PartialCityAverages,
    PartialYearCounts,
    covariant=True,
)
GenericAggregatedRecordContr = TypeVar(
    "GenericAggregatedRecordContr",
    PartialRainAverages,
    PartialCityAverages,
    PartialYearCounts,
    contravariant=True,
)
