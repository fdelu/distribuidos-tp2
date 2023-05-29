from abc import ABC, abstractmethod
from typing import Generic

from common.messages.basic import (
    BasicRecord,
    BasicStation,
    BasicTrip,
    BasicWeather,
    BasicDataRecordHandler,
)
from common.messages.joined import GenericJoinedTrip

from ..config import Config
from ..comms import JoinerComms

Joiner = BasicDataRecordHandler

__all__ = ["GenericJoinedTrip"]


class Phase(ABC, Generic[GenericJoinedTrip]):
    comms: JoinerComms[GenericJoinedTrip]
    config: Config
    joiner: BasicDataRecordHandler[None]

    def __init__(
        self,
        comms: JoinerComms[GenericJoinedTrip],
        config: Config,
        joiner: BasicDataRecordHandler[None],
    ):
        self.comms = comms
        self.config = config
        self.joiner = joiner

    @abstractmethod
    def handle_weather(self, weather: BasicWeather) -> "Phase[GenericJoinedTrip]":
        ...

    @abstractmethod
    def handle_station(self, station: BasicStation) -> "Phase[GenericJoinedTrip]":
        ...

    @abstractmethod
    def handle_trips_start(self) -> "Phase[GenericJoinedTrip]":
        ...

    @abstractmethod
    def handle_trip(self, trip: BasicTrip) -> "Phase[GenericJoinedTrip]":
        ...

    @abstractmethod
    def handle_end(self) -> "Phase[GenericJoinedTrip]":
        ...

    def handle_record(self, record: BasicRecord) -> "Phase[GenericJoinedTrip]":
        return record.be_handled_by(self)
