import signal
from uuid import uuid4
from threading import Event
from typing import Any, Iterable
import zmq


from shared.messages import StatType


from .config import BikeRidesAnalyzerConfig
from .comms.input import CommsInput
from .comms.output import CommsOutput
from .phase import Phase
from .phase.weather_stations import WeatherStationsPhase

TIMEOUT_MILLISECONDS = 1000


class BikeRidesAnalyzer:
    phase: Phase
    interrupted: Event
    context: zmq.Context[zmq.Socket[None]]
    job_id: str

    def __init__(
        self, config: BikeRidesAnalyzerConfig, job_id: str | None = None
    ) -> None:
        self.context = zmq.Context()
        self.context.setsockopt(zmq.LINGER, 0)  # Don't block on close

        self.interrupted = Event()
        self.job_id = job_id or uuid4().hex

        comms_input = CommsInput(self.job_id, self.context, config, self.interrupted)
        comms_output = CommsOutput(self.job_id, self.context, config, self.interrupted)
        self.phase = WeatherStationsPhase(comms_input, comms_output)

    def interrupt_on_signal(self, signum: signal.Signals) -> None:
        signal.signal(signum, lambda *_: self.interrupted.set())

    def send_stations(self, city: str, lines: Iterable[str]) -> None:
        self.phase = self.phase.send_stations(city, lines)

    def send_weather(self, city: str, lines: Iterable[str]) -> None:
        self.phase = self.phase.send_weather(city, lines)

    def send_trips(self, city: str, lines: Iterable[str]) -> None:
        self.phase = self.phase.send_trips(city, lines)

    def get_rain_averages(self) -> dict[str, float]:
        return self.__get_stat(StatType.RAIN)

    def get_year_counts(self) -> dict[str, list[int]]:
        return self.__get_stat(StatType.YEAR)

    def get_city_averages(self) -> dict[str, float]:
        return self.__get_stat(StatType.CITY)

    def close(self) -> None:
        self.phase.close()
        self.context.term()

    def __get_stat(self, stat_type: StatType) -> dict[str, Any]:
        self.phase, stat = self.phase.get_stat(stat_type)
        return {x: y for x, y in sorted(stat.items())}
