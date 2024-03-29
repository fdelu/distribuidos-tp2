from haversine import haversine  # type: ignore

from common.messages.joined import JoinedCityTrip
from common.messages.aggregated import PartialCityAverages, StationInfo
from common.persistence import WithState

Averages = dict[str, StationInfo]  # station -> count


class CityAggregator(WithState[Averages]):
    def __init__(self) -> None:
        super().__init__({})

    def handle_joined(self, trip: JoinedCityTrip) -> None:
        station_average = self.state.setdefault(
            trip.end_station_name, StationInfo(0, 0)
        )
        station_average.count += 1
        distance: float = haversine(
            trip.start_station_coordinates, trip.end_station_coordinates
        )
        delta = (distance - station_average.average_distance) / station_average.count
        station_average.average_distance += delta

    def get_value(self) -> PartialCityAverages | None:
        if len(self.state) == 0:
            return None
        return PartialCityAverages(self.state)

    def reset(self) -> None:
        self.state = {}
