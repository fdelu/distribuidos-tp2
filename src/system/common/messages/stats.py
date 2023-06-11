from shared.messages import (
    StatType,
    RainAverages,
    YearCounts,
    CityAverages,
)

__all__ = ["StatType", "RainAverages", "YearCounts", "CityAverages"]


StatsRecord = RainAverages | YearCounts | CityAverages
