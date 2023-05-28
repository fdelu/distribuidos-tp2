from common.messages.joined import JoinedCityTrip
from common.messages.aggregated import PartialCityAverages, StationInfo
from haversine import haversine  # type: ignore


class CityAggregator:
    averages: dict[str, StationInfo]  # station -> count

    def __init__(self) -> None:
        self.averages = {}

    def handle_joined(self, trip: JoinedCityTrip) -> None:
        station_average = self.averages.setdefault(
            trip.end_station_name, StationInfo(0, 0)
        )
        station_average.count += 1
        distance: float = haversine(
            trip.start_station_coordinates, trip.end_station_coordinates
        )
        delta = (distance - station_average.average_distance) / station_average.count
        station_average.average_distance += delta

    def get_value(self) -> PartialCityAverages:
        return PartialCityAverages(self.averages)

    def reset(self) -> None:
        self.averages = {}
