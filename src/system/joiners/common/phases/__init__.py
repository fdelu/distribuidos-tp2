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


class Phase(ABC, Generic[GenericJoinedTrip]):
    comms: JoinerComms[GenericJoinedTrip]
    config: Config
    joiner: BasicDataRecordHandler

    def __init__(
        self, comms: JoinerComms, config: Config, joiner: BasicDataRecordHandler
    ):
        self.comms = comms
        self.config = config
        self.joiner = joiner

    @abstractmethod
    def handle_weather(self, weather: BasicWeather) -> "Phase":
        ...

    @abstractmethod
    def handle_station(self, station: BasicStation) -> "Phase":
        ...

    @abstractmethod
    def handle_trips_start(self) -> "Phase":
        ...

    @abstractmethod
    def handle_trip(self, trip: BasicTrip) -> "Phase":
        ...

    @abstractmethod
    def handle_end(self) -> "Phase":
        ...

    def handle_record(self, record: BasicRecord) -> "Phase":
        return record.be_handled_by(self)
