from dataclasses import dataclass
from typing import Protocol, TypeVar

# Re-export
from shared.messages import StatType  # noqa

T = TypeVar("T", covariant=True)


@dataclass
class RainAverages:
    data: dict[str, float]  # date -> average duration

    def be_handled_by(self, handler: "StatHandler[T]") -> T:
        return handler.handle_rain_averages(self)


@dataclass
class YearCounts:
    # station -> (count year base, count year duplicated)
    data: dict[str, tuple[int, int]]

    def be_handled_by(self, handler: "StatHandler[T]") -> T:
        return handler.handle_year_counts(self)


@dataclass
class CityAverages:
    data: dict[str, float]  # station -> average distance

    def be_handled_by(self, handler: "StatHandler[T]") -> T:
        return handler.handle_city_averages(self)


class StatHandler(Protocol[T]):
    def handle_rain_averages(self, averages: RainAverages) -> T:
        ...

    def handle_year_counts(self, counts: YearCounts) -> T:
        ...

    def handle_city_averages(self, averages: CityAverages) -> T:
        ...


StatsRecord = RainAverages | YearCounts | CityAverages
