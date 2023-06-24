from dataclasses import dataclass

from common.messages.aggregated import PartialYearCounts
from common.messages.stats import YearCounts, StatsRecord
from common.persistence import WithState

from .config import Config


@dataclass
class Counts:
    # station name -> count
    counts_year_base: dict[str, int]
    counts_year_compared: dict[str, int]


class YearReducer(WithState[Counts]):
    def __init__(self) -> None:
        super().__init__(Counts({}, {}))

    def handle_aggregated(self, counts: PartialYearCounts) -> None:
        self.__merge_counts(self.state.counts_year_base, counts.counts_year_base)
        self.__merge_counts(
            self.state.counts_year_compared, counts.counts_year_compared
        )

    def __merge_counts(
        self, counts: dict[str, int], other_counts: dict[str, int]
    ) -> None:
        for station, count in other_counts.items():
            counts[station] = counts.get(station, 0) + count

    def get_value(self) -> StatsRecord:
        result = {}
        for station, count_year_compared in self.state.counts_year_compared.items():
            count_year_base = self.state.counts_year_base.get(station, None)
            if (
                count_year_base is not None
                and count_year_compared > count_year_base * Config().factor
            ):
                result[station] = (count_year_base, count_year_compared)
        return YearCounts(result)
