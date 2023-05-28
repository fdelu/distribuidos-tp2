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

    def handle_station(self, station: BasicStation) -> Phase:
        logging.warn("Unexpected Station received while receiving trips")
        return self

    def handle_weather(self, weather: BasicWeather) -> Phase:
        logging.warn("Unexpected Weather received while receiving trips")
        return self

    def handle_trips_start(self) -> Phase:
        logging.warn("Unexpected TripsStart received while already receiving trips")
        return self

    def handle_trip(self, trip: BasicTrip) -> Phase:
        self.joiner.handle_trip(trip)
        self.count += 1
        return self

    def handle_end(self) -> Phase:
        self.ends_received += 1
        logging.debug(
            "A parser finished sending trips"
            f" ({self.ends_received}/{self.config.parsers_count})"
        )
        if self.ends_received < self.config.parsers_count:
            return self
        logging.info("All parsers finished sending trips, waiting until all are joined")
        self.comms.set_all_trips_done_callback(self._all_trips_done)
        return self

    def _all_trips_done(self):
        logging.info(
            f"Finished joining all trips. Total processed in this node: {self.count}"
        )
        self.comms.send(End())
        self.comms.stop_consuming()
