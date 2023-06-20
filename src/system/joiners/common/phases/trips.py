import logging
from typing import Generic

from common.messages import End, Start
from common.messages.basic import (
    BasicStation,
    BasicTrip,
    BasicWeather,
)
from common.persistence import StatePersistor

from . import Phase, GenericJoinedTrip


class TripsPhase(Phase[GenericJoinedTrip], Generic[GenericJoinedTrip]):
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
        self.state.count += 1
        return self

    def handle_end(self, end: End) -> Phase[GenericJoinedTrip]:
        super().handle_end(end)
        self.check_ends()
        return self

    def check_ends(self) -> None:
        if len(self.state.ends_received) < self.config.parsers_count:
            return

        logging.info(
            f"Job {self.job_id} | All parsers finished sending trips, waiting until all"
            " are joined"
        )
        self.comms.set_all_trips_done_callback(self.job_id, self._all_trips_done)

    def _all_trips_done(self) -> None:
        logging.info(
            f"Job {self.job_id} | Finished joining all trips. Total processed in this"
            f" node: {self.state.count}"
        )
        self._send(End(self.comms.id))
        self.comms.stop_consuming_trips(self.job_id)
        StatePersistor().remove(self._control_store_key())
        StatePersistor().remove(self._joiner_store_key())
        self.on_finish(self.job_id)

    def __warn(self, name: str) -> None:
        logging.warn(
            f"Job {self.job_id} | Unexpected {name} received while receiving trips"
        )
