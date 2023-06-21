import logging
from typing import Any, Iterable

from shared.messages import AllSent, RecordType, StatType

from . import Phase
from .end import EndPhase


class TripsPhase(Phase):
    def send_stations(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send stations in trips phase")

    def send_weather(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send weather in trips phase")

    def send_trips(self, city: str, lines: Iterable[str]) -> Phase:
        logging.info(f"Sending trips for {city}")
        self.input.send_batchs(city, lines, RecordType.TRIP)
        return self

    def get_stat(self, stat_type: StatType) -> tuple[Phase, Any]:
        self.input.send(AllSent())
        self.input.recv_ack()
        next_phase = EndPhase(self.input, self.output)
        return (*next_phase.get_stat(stat_type),)
