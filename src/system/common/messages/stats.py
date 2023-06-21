from dataclasses import dataclass
from shared.messages import (
    StatType,
    RainAverages as RainAveragesBase,
    YearCounts as YearCountsBase,
    CityAverages as CityAveragesBase,
)

__all__ = ["StatType"]


@dataclass
class StatBase:
    def get_routing_key(self) -> str:
        return "stat"


@dataclass
class RainAverages(StatBase, RainAveragesBase):
    ...


@dataclass
class YearCounts(StatBase, YearCountsBase):
    ...


@dataclass
class CityAverages(StatBase, CityAveragesBase):
    ...


StatsRecord = RainAverages | YearCounts | CityAverages
