from abc import ABC, abstractmethod
import logging
from typing import Generic, Callable, Protocol
from dataclasses import dataclass

from common.messages import End, Message, Start
from common.messages.basic import (
    BasicStation,
    BasicTrip,
    BasicWeather,
)
from common.messages.joined import GenericJoinedTrip, GenericJoinedTripCov
from common.messages.basic import (
    BasicTripHandler,
    BasicWeatherHandler,
    BasicStationHandler,
)
from common.persistence import WithState, WithStateProtocol

from ..config import Config
from ..comms import JoinerComms

__all__ = ["GenericJoinedTrip"]


class Joiner(
    BasicStationHandler[None],
    BasicWeatherHandler[None],
    BasicTripHandler[GenericJoinedTripCov | None],
    WithStateProtocol,
    Protocol,
):
    ...


@dataclass
class State:
    starts_received: set[str]
    ends_received: set[str]
    count: int
    trips_phase: bool


class Phase(ABC, Generic[GenericJoinedTrip], WithState[State]):
    comms: JoinerComms[GenericJoinedTrip]
    config: Config
    joiner: Joiner[GenericJoinedTrip]
    job_id: str
    on_finish: Callable[[str], None]

    def __init__(
        self,
        comms: JoinerComms[GenericJoinedTrip],
        config: Config,
        joiner: Joiner[GenericJoinedTrip],
        job_id: str,
        on_finish: Callable[[str], None],
        state: State | None = None,
    ):
        super().__init__(state or State(set(), set(), 0, False))
        self.comms = comms
        self.config = config
        self.joiner = joiner
        self.job_id = job_id
        self.on_finish = on_finish

    @abstractmethod
    def handle_weather(self, weather: BasicWeather) -> "Phase[GenericJoinedTrip]":
        raise NotImplementedError()

    @abstractmethod
    def handle_station(self, station: BasicStation) -> "Phase[GenericJoinedTrip]":
        raise NotImplementedError()

    @abstractmethod
    def handle_start(self, start: Start) -> "Phase[GenericJoinedTrip]":
        raise NotImplementedError()

    @abstractmethod
    def handle_trip(self, trip: BasicTrip) -> "Phase[GenericJoinedTrip]":
        raise NotImplementedError()

    def handle_end(self, end: End) -> "Phase[GenericJoinedTrip]":
        if end.host is None:
            logging.warn("Received End without host id")
            return self
        self.state.ends_received.add(end.host)
        logging.debug(
            f"Job {self.job_id} | Parser {end.host} finished sending trips"
            f" ({len(self.state.ends_received)}/{self.config.parsers_count})"
        )
        return self

    def store_state(self) -> "Phase[GenericJoinedTrip]":
        self.store_to(self._phase_store_key())
        self.joiner.store_to(self._joiner_store_key())
        return self

    def _send(self, record: GenericJoinedTrip | End | Start) -> None:
        self.comms.send(Message(self.job_id, record))

    def _phase_store_key(self) -> str:
        return f"phase_{self.job_id}"

    def _joiner_store_key(self) -> str:
        return f"joiner_{self.job_id}"
