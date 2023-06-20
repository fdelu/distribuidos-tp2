import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedRainTrip
from common.persistence import WithState

from .config import Config

# city -> day -> precipitation
Weather = dict[str, dict[str, float]]


class RainJoiner(WithState[Weather]):
    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__({})
        self.config = config

    def handle_station(self, station: BasicStation) -> None:
        logging.warn("Unexpected Station received on rain joiner")

    def handle_weather(self, weather: BasicWeather) -> None:
        weathers = self.state.setdefault(weather.city, {})
        weathers[weather.date] = weather.precipitation

    def handle_trip(self, trip: BasicTrip) -> JoinedRainTrip | None:
        precipitation = self._get_join_data(trip)
        if (
            precipitation is None
            or precipitation <= self.config.precipitation_threshold
        ):
            return None

        return JoinedRainTrip(trip.start_date, trip.duration_sec)

    def _get_join_data(self, trip: BasicTrip) -> float | None:
        weather = self.state[trip.city].get(trip.start_date, None)
        if weather is None:
            logging.debug(f"Missing weather for date {trip.start_date} ({trip.city})")
            return None
        return weather
