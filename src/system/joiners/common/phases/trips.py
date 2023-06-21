import logging

from common.messages import End, Start
from common.messages.basic import (
    BasicStation,
    BasicTrip,
    BasicWeather,
)
from ..phases import Phase, GenericJoinedTrip


class TripsPhase(Phase[GenericJoinedTrip]):
    ends_received: set[str] | None = None
    count: int = 0

    def handle_station(self, station: BasicStation) -> Phase[GenericJoinedTrip]:
        self.__warn("Station")
        return self

    def handle_weather(self, weather: BasicWeather) -> Phase[GenericJoinedTrip]:
        self.__warn("Weather")
        return self

    def handle_start(self, start: Start) -> Phase[GenericJoinedTrip]:
        self.__warn(f"Start (host id: {start.host})")
        return self

    def handle_trip(self, trip: BasicTrip) -> Phase[GenericJoinedTrip]:
        joined_trip = self.joiner.handle_trip(trip)
        if joined_trip is not None:
            self._send(joined_trip)
        self.count += 1
        return self

    def handle_end(self, end: End) -> Phase[GenericJoinedTrip]:
        if end.host is None:
            logging.warn("Received End without host id")
            return self
        self.ends_received = self.ends_received or set()
        self.ends_received.add(end.host)
        logging.debug(
            f"Job {self.job_id} | Parser {end.host} finished sending trips"
            f" ({len(self.ends_received)}/{self.config.parsers_count})"
        )
        if len(self.ends_received) < self.config.parsers_count:
            return self
        logging.info(
            f"Job {self.job_id} | All parsers finished sending trips, waiting until all"
            " are joined"
        )
        self.comms.set_all_trips_done_callback(self.job_id, self._all_trips_done)
        return self

    def _all_trips_done(self) -> None:
        logging.info(
            f"Job {self.job_id} | Finished joining all trips. Total processed in this"
            f" node: {self.count}"
        )
        self._send(End(self.comms.id))
        self.comms.stop_consuming_trips(self.job_id)
        self.on_finish(self)

    def __warn(self, name: str) -> None:
        logging.warn(
            f"Job {self.job_id} | Unexpected {name} received while receiving trips"
        )
