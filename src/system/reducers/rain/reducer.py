from common.messages.aggregated import DateInfo, PartialRainAverages
from common.messages.stats import RainAverages, StatsRecord
from common.persistence import WithState

Averages = dict[str, DateInfo]  # date -> info


class RainReducer(WithState[Averages]):
    def __init__(self) -> None:
        super().__init__({})

    def handle_aggregated(self, avg: PartialRainAverages) -> None:
        for date, date_average in avg.duration_averages.items():
            current = self.state.setdefault(date, DateInfo(0, 0))

            total_count = current.count + date_average.count
            current_factor = current.count / total_count
            new_factor = date_average.count / total_count

            current.average_duration = (
                current.average_duration * current_factor
                + date_average.average_duration * new_factor
            )
            current.count = total_count

    def get_value(self) -> StatsRecord:
        return RainAverages({x: y.average_duration for x, y in self.state.items()})
