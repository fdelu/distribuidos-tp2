import logging

from common.messages import End
from common.messages.basic import (
    BasicStation,
    BasicTrip,
    BasicWeather,
)
from ..phases import Phase, GenericJoinedTrip


class TripsPhase(Phase[GenericJoinedTrip]):
    ends_received: int = 0
    count: int = 0

    def handle_station(self, station: BasicStation) -> Phase[GenericJoinedTrip]:
        logging.warn("Unexpected Station received while receiving trips")
        return self

    def handle_weather(self, weather: BasicWeather) -> Phase[GenericJoinedTrip]:
        logging.warn("Unexpected Weather received while receiving trips")
        return self

    def handle_start(self) -> Phase[GenericJoinedTrip]:
        logging.warn("Unexpected Start received while already receiving trips")
        return self

    def handle_trip(self, trip: BasicTrip) -> Phase[GenericJoinedTrip]:
        joined_trip = self.joiner.handle_trip(trip)
        if joined_trip is not None:
            self._send(joined_trip)
        self.count += 1
        return self

    def handle_end(self) -> Phase[GenericJoinedTrip]:
        self.ends_received += 1
        logging.debug(
            f"Job {self.job_id} | A parser finished sending trips"
            f" ({self.ends_received}/{self.config.parsers_count})"
        )
        if self.ends_received < self.config.parsers_count:
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
        self._send(End())
        self.comms.stop_consuming_trips(self.job_id)
        self.on_finish(self)
