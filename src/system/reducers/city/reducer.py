from common.messages.aggregated import PartialCityAverages, StationInfo
from common.messages.stats import CityAverages, StatsRecord
from common.persistence import WithState

from .config import Config

Averages = dict[str, StationInfo]  # station name -> info


class CityReducer(WithState[Averages]):
    def __init__(self) -> None:
        super().__init__({})

    def handle_aggregated(self, avg: PartialCityAverages) -> None:
        for station, station_average in avg.distance_averages.items():
            current = self.state.setdefault(station, StationInfo(0, 0))

            total_count = current.count + station_average.count
            current_factor = current.count / total_count
            new_factor = station_average.count / total_count

            current.average_distance = (
                current.average_distance * current_factor
                + station_average.average_distance * new_factor
            )
            current.count = total_count

    def get_value(self) -> StatsRecord:
        result = {}
        for name, average in self.state.items():
            if average.average_distance >= Config().min_distance_km:
                result[name] = average.average_distance
        return CityAverages(result)
