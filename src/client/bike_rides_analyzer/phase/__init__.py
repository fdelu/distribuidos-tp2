from abc import ABC, abstractmethod
from typing import Iterable, Any

from shared.messages import StatType

from ..comms.input import CommsInput
from ..comms.output import CommsOutput


class Phase(ABC):
    input: CommsInput
    output: CommsOutput

    def __init__(self, input: CommsInput, output: CommsOutput) -> None:
        self.input = input
        self.output = output

    @abstractmethod
    def send_stations(self, city: str, lines: Iterable[str]) -> "Phase":
        ...

    @abstractmethod
    def send_weather(self, city: str, lines: Iterable[str]) -> "Phase":
        ...

    @abstractmethod
    def send_trips(self, city: str, lines: Iterable[str]) -> "Phase":
        ...

    @abstractmethod
    def get_stat(self, stat_type: StatType) -> tuple["Phase", Any]:
        ...

    def close(self) -> None:
        self.input.close()
        self.output.close()
