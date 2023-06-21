import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedRainTrip

from .config import Config


class RainJoiner:
    # city -> day -> precipitation
    weather: dict[str, dict[str, float]]
    config: Config

    def __init__(self, config: Config) -> None:
        self.weather = {}
        self.config = config

    def handle_station(self, station: BasicStation) -> None:
        logging.warn("Unexpected Station received on rain joiner")

    def handle_weather(self, weather: BasicWeather) -> None:
        weathers = self.weather.setdefault(weather.city, {})
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
        weather = self.weather[trip.city].get(trip.start_date, None)
        if weather is None:
            logging.debug(f"Missing weather for date {trip.start_date} ({trip.city})")
            return None
        return weather
