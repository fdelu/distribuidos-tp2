import logging
from typing import Any, Iterable

from shared.messages import StatType, GetStat, Ack

from . import Phase


class EndPhase(Phase):
    def send_stations(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send stations, job is done")

    def send_weather(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send weather, job is done")

    def send_trips(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send trips, job is done")

    def get_stat(self, stat_type: StatType) -> tuple[Phase, Any]:
        self.output.send(GetStat(stat_type))
        logging.debug(f"Requesting stat {stat_type}")
        response = self.output.recv()
        if isinstance(response, Ack) or response.stat_type() != stat_type:
            raise RuntimeError("Did not receive stat from server")
        logging.debug(f"Stat {stat_type} received")
        return self, response.data
