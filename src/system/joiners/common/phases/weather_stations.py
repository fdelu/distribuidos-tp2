import logging
from typing import Generic

from common.messages import End, TripsStart, Start
from common.messages.basic import (
    BasicStation,
    BasicTrip,
    BasicWeather,
)

from . import Phase, GenericJoinedTrip
from .trips import TripsPhase


class WeatherStationsPhase(Phase[GenericJoinedTrip], Generic[GenericJoinedTrip]):
    def handle_station(self, station: BasicStation) -> Phase[GenericJoinedTrip]:
        self.joiner.handle_station(station)
        return self

    def handle_weather(self, weather: BasicWeather) -> Phase[GenericJoinedTrip]:
        self.joiner.handle_weather(weather)
        return self

    def handle_trips_start(self, start: TripsStart) -> Phase[GenericJoinedTrip]:
        if start.host is None:
            logging.warn("Received TripsStart without host id")
            return self
        return self.__handle_trips_start(start.host)

    def __handle_trips_start(self, host: str) -> Phase[GenericJoinedTrip]:
        self.state.starts_received.add(host)
        logging.debug(
            f"Job {self.job_id} | Parser {host} finished sending weather &"
            f" stations ({len(self.state.starts_received)}/{self.config.parsers_count})"
        )
        if len(self.state.starts_received) < self.config.parsers_count:
            return self

        logging.info(f"Job {self.job_id} | Receiving trips")
        self.state.trips_phase = True
        self.store_state()
        self._send(Start(self.comms.id))
        return self.__next_phase()

    def handle_trip(self, trip: BasicTrip) -> Phase[GenericJoinedTrip]:
        logging.warn(
            f"Job {self.job_id} | Unexpected Trip received while receiving weather &"
            " stations"
        )
        return self

    def handle_end(self, end: End) -> Phase[GenericJoinedTrip]:
        super().handle_end(end)
        logging.debug(
            f"Job {self.job_id} | Received End from parser {end.host} before all Starts"
        )
        if end.host is not None and end.host not in self.state.starts_received:
            logging.debug(
                f"Job {self.job_id} | Received End from parser {end.host} before Start,"
                " considering it as both"
            )
            return self.__handle_trips_start(end.host)
        return self

    def restore_state(self) -> "Phase[GenericJoinedTrip]":
        self.restore_from(self._control_store_key())
        self.joiner.restore_from(self._joiner_store_key())

        if self.state.trips_phase:
            return self.__next_phase()
        return self

    def __next_phase(self) -> TripsPhase[GenericJoinedTrip]:
        self.comms.start_consuming_trips(self.job_id)
        phase = TripsPhase(
            self.comms,
            self.config,
            self.joiner,
            self.job_id,
            self.on_finish,
            self.state,
        )
        phase.check_ends()
        return phase
