import logging
import time
from typing import Any, Iterable

from shared.messages import StatType, GetStat, NotAvailable, ServerMessagesOutput

from . import Phase

RETRY_DELAY = 3


class EndPhase(Phase):
    def send_stations(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send stations, job is done")

    def send_weather(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send weather, job is done")

    def send_trips(self, city: str, lines: Iterable[str]) -> Phase:
        raise ValueError("Can't send trips, job is done")

    def get_stat(self, stat_type: StatType) -> tuple[Phase, Any]:
        logging.debug(f"Requesting stat {stat_type}")
        response: ServerMessagesOutput | None = None
        while response is None or isinstance(response, NotAvailable):
            if response is not None:
                time.sleep(RETRY_DELAY)
            self.output.send(GetStat(stat_type))
            response = self.output.recv()
        logging.debug(f"Stat {stat_type} received")
        return self, response.data
