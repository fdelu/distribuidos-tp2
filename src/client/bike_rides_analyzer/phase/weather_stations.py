import logging
from typing import Any, Iterable

from shared.messages import RecordType, StatType

from . import Phase
from .trips import TripsPhase


class WeatherStationsPhase(Phase):
    def send_stations(self, city: str, lines: Iterable[str]) -> Phase:
        logging.info(f"Sending stations for {city}")
        self.input.send_batchs(city, lines, RecordType.STATION)
        return self

    def send_weather(self, city: str, lines: Iterable[str]) -> Phase:
        logging.info(f"Sending weather for {city}")
        self.input.send_batchs(city, lines, RecordType.WEATHER)
        return self

    def send_trips(self, city: str, lines: Iterable[str]) -> Phase:
        next_phase = TripsPhase(self.input, self.output)
        return next_phase.send_trips(city, lines)

    def get_stat(self, stat_type: StatType) -> tuple[Phase, Any]:
        raise ValueError("Can't get stat in weather stations phase")
