import logging

from common.messages.basic import (
    BasicStation,
    BasicTrip,
    BasicWeather,
)

from . import Phase, GenericJoinedTrip
from .trips import TripsPhase


class WeatherStationsPhase(Phase[GenericJoinedTrip]):
    parsers_sending_trips: int = 0
    ends_received: int = 0

    def handle_station(self, station: BasicStation) -> Phase[GenericJoinedTrip]:
        self.joiner.handle_station(station)
        return self

    def handle_weather(self, weather: BasicWeather) -> Phase[GenericJoinedTrip]:
        self.joiner.handle_weather(weather)
        return self

    def handle_trips_start(self) -> Phase[GenericJoinedTrip]:
        self.parsers_sending_trips += 1
        logging.debug(
            "A parser finished sending weather & stations"
            f" ({self.parsers_sending_trips}/{self.config.parsers_count})"
        )
        if self.parsers_sending_trips < self.config.parsers_count:
            return self

        logging.info("Receiving trips")
        self.comms.start_consuming_trips()
        trips_phase: Phase[GenericJoinedTrip] = TripsPhase(
            self.comms, self.config, self.joiner
        )
        for _ in range(self.ends_received):
            trips_phase = trips_phase.handle_end()
        return trips_phase

    def handle_trip(self, trip: BasicTrip) -> Phase[GenericJoinedTrip]:
        logging.warn("Unexpected Trip received while receiving weather & stations")
        return self

    def handle_end(self) -> Phase[GenericJoinedTrip]:
        self.ends_received += 1
        return self
