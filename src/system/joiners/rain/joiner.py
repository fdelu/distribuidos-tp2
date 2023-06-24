import logging

from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.joined import JoinedRainTrip
from common.persistence import WithStateAppended

from .config import Config

# (city, day) -> precipitation
Key = tuple[str, str]
Value = float


class RainJoiner(WithStateAppended[Key, Value]):
    def handle_station(self, station: BasicStation) -> None:
        logging.warn("Unexpected Station received on rain joiner")

    def handle_weather(self, weather: BasicWeather) -> None:
        self.set((weather.city, weather.date), weather.precipitation)

    def handle_trip(self, trip: BasicTrip) -> JoinedRainTrip | None:
        precipitation = self._get_join_data(trip)
        if precipitation is None or precipitation <= Config().precipitation_threshold:
            return None

        return JoinedRainTrip(trip.start_date, trip.duration_sec)

    def _get_join_data(self, trip: BasicTrip) -> float | None:
        weather = self.state.get((trip.city, trip.start_date), None)
        if weather is None:
            logging.debug(f"Missing weather for date {trip.start_date} ({trip.city})")
            return None
        return weather
