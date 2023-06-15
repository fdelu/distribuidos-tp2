import logging

from common.messages import Start, Message
from common.messages.raw import RawLines

from . import Phase
from .trips import TripsPhase
from .parse import parse_weather, parse_station


class WeatherStationsPhase(Phase):
    received_end: bool = False

    def handle_start(self) -> Phase:
        self.comms.start_consuming_job(self.job_id)
        return self

    def handle_station_batch(self, batch: RawLines) -> Phase:
        self._send_parsed(batch, parse_station)
        return self

    def handle_weather_batch(self, batch: RawLines) -> Phase:
        self._send_parsed(batch, parse_weather)
        return self

    def handle_trip_batch(self, batch: RawLines) -> Phase:
        self.comms.send(Message(self.job_id, Start()), force_msg_id=None)
        trips_phase: Phase = TripsPhase(self.comms, self.job_id, self.on_finish)
        trips_phase = trips_phase.handle_trip_batch(batch)
        if self.received_end:
            trips_phase = trips_phase.handle_end()
        return trips_phase

    def handle_end(self) -> Phase:
        self.received_end = True
        logging.debug(
            f"Job {self.job_id} | Received End before finishing with Weather & Stations"
        )
        return self
