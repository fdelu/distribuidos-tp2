from common.messages.aggregated import PartialYearCounts
from common.messages.stats import YearCounts, StatsRecord
from .config import Config


class YearReducer:
    counts_year_base: dict[str, int]  # station name -> count
    counts_year_compared: dict[str, int]  # station name -> count
    config: Config

    def __init__(self, config: Config) -> None:
        self.counts_year_base = {}
        self.counts_year_compared = {}
        self.config = config

    def handle_aggregated(self, counts: PartialYearCounts) -> None:
        self.__merge_counts(self.counts_year_base, counts.counts_year_base)
        self.__merge_counts(self.counts_year_compared, counts.counts_year_compared)

    def __merge_counts(
        self, counts: dict[str, int], other_counts: dict[str, int]
    ) -> None:
        for station, count in other_counts.items():
            counts[station] = counts.get(station, 0) + count

    def get_value(self) -> StatsRecord:
        result = {}
        for station, count_year_compared in self.counts_year_compared.items():
            count_year_base = self.counts_year_base.get(station, None)
            if (
                count_year_base is not None
                and count_year_compared > count_year_base * self.config.factor
            ):
                result[station] = (count_year_base, count_year_compared)
        return YearCounts(result)
