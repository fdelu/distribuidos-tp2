import logging

from common.messages import End, Start
from common.messages.basic import (
    BasicStation,
    BasicTrip,
    BasicWeather,
)

from . import Phase, GenericJoinedTrip
from .trips import TripsPhase


class WeatherStationsPhase(Phase[GenericJoinedTrip]):
    starts_received: set[str] | None = None
    ends_received: list[End] | None = None

    def handle_station(self, station: BasicStation) -> Phase[GenericJoinedTrip]:
        self.joiner.handle_station(station)
        return self

    def handle_weather(self, weather: BasicWeather) -> Phase[GenericJoinedTrip]:
        self.joiner.handle_weather(weather)
        return self

    def handle_start(self, start: Start) -> Phase[GenericJoinedTrip]:
        if start.host is None:
            logging.warn("Received Start without host id")
            return self
        self.starts_received = self.starts_received or set()
        self.starts_received.add(start.host)
        logging.debug(
            f"Job {self.job_id} | Parser {start.host} finished sending weather &"
            f" stations ({len(self.starts_received)}/{self.config.parsers_count})"
        )
        if len(self.starts_received) < self.config.parsers_count:
            return self

        logging.info(f"Job {self.job_id} | Receiving trips")
        self.comms.start_consuming_trips(self.job_id)
        self._send(Start(self.comms.id))
        trips_phase: Phase[GenericJoinedTrip] = TripsPhase(
            self.comms, self.config, self.joiner, self.job_id, self.on_finish
        )
        for end in self.ends_received or []:
            trips_phase = trips_phase.handle_end(end)
        return trips_phase

    def handle_trip(self, trip: BasicTrip) -> Phase[GenericJoinedTrip]:
        logging.warn(
            f"Job {self.job_id} | Unexpected Trip received while receiving weather &"
            " stations"
        )
        return self

    def handle_end(self, end: End) -> Phase[GenericJoinedTrip]:
        logging.debug(
            f"Job {self.job_id} | Received End from parser {end.host} before all Starts"
        )
        self.ends_received = self.ends_received or []
        self.ends_received.append(end)
        return self
