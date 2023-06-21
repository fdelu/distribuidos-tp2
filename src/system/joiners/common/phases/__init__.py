from abc import ABC, abstractmethod
from typing import Generic, Callable, Protocol

from common.messages import End, Message, Start
from common.messages.basic import (
    BasicRecord,
    BasicStation,
    BasicTrip,
    BasicWeather,
    BasicTripHandler,
    BasicWeatherHandler,
    BasicStationHandler,
)
from common.messages.joined import GenericJoinedTrip, GenericJoinedTripCov

from ..config import Config
from ..comms import JoinerComms


class Joiner(
    BasicStationHandler[None],
    BasicWeatherHandler[None],
    BasicTripHandler[GenericJoinedTripCov | None],
    Protocol,
):
    ...


__all__ = ["GenericJoinedTrip"]


class Phase(ABC, Generic[GenericJoinedTrip]):
    comms: JoinerComms[GenericJoinedTrip]
    config: Config
    joiner: Joiner[GenericJoinedTrip]
    on_finish: Callable[["Phase[GenericJoinedTrip]"], None]
    job_id: str

    def __init__(
        self,
        comms: JoinerComms[GenericJoinedTrip],
        config: Config,
        joiner: Joiner[GenericJoinedTrip],
        job_id: str,
        on_finish: Callable[["Phase[GenericJoinedTrip]"], None],
    ):
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

    @abstractmethod
    def handle_end(self, end: End) -> "Phase[GenericJoinedTrip]":
        raise NotImplementedError()

    def _send(self, record: GenericJoinedTrip | End | Start) -> None:
        self.comms.send(Message(self.job_id, record))
