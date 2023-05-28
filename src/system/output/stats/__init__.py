from threading import Lock
from dataclasses import dataclass

from common.messages.stats import RainAverages, CityAverages, YearCounts


@dataclass
class Stats:
    rain_averages: RainAverages | None = None
    year_counts: YearCounts | None = None
    city_averages: CityAverages | None = None
    lock: Lock = Lock()

    def all_done(self):
        return (
            self.rain_averages is not None
            and self.year_counts is not None
            and self.city_averages is not None
        )
