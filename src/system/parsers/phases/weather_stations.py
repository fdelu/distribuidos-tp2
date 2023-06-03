from common.messages.basic import TripsStart
from common.messages.raw import RawLines

from . import Phase
from .trips import TripsPhase
from .parse import parse_weather, parse_station


class WeatherStationsPhase(Phase):
    received_end: bool = False

    def handle_station_batch(self, batch: RawLines) -> Phase:
        self._send_parsed(batch, parse_station)
        return self

    def handle_weather_batch(self, batch: RawLines) -> Phase:
        self._send_parsed(batch, parse_weather)
        return self

    def handle_trip_batch(self, batch: RawLines) -> Phase:
        self._send(TripsStart())
        trips_phase: Phase = TripsPhase(self.comms, self.job_id, self.on_finish)
        trips_phase = trips_phase.handle_trip_batch(batch)
        if self.received_end:
            trips_phase = trips_phase.handle_end()
        return trips_phase

    def handle_end(self) -> Phase:
        self.received_end = True
        return self
