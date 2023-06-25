from common.messages.joined import JoinedRainTrip
from common.messages.aggregated import DateInfo, PartialRainAverages
from common.persistence import WithState

Averages = dict[str, DateInfo]  # date -> DateInfo


class RainAggregator(WithState[Averages]):
    def __init__(self) -> None:
        super().__init__({})

    def handle_joined(self, trip: JoinedRainTrip) -> None:
        date_average = self.state.setdefault(trip.start_date, DateInfo(0, 0))
        date_average.count += 1
        delta = (trip.duration_sec - date_average.average_duration) / date_average.count
        date_average.average_duration += delta

    def get_value(self) -> PartialRainAverages | None:
        if len(self.state) == 0:
            return None
        return PartialRainAverages(self.state)

    def reset(self) -> None:
        self.state = {}
