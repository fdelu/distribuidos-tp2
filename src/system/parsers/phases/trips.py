import logging

from common.messages import End
from common.messages.raw import RawLines

from . import Phase
from .parse import parse_trip


class TripsPhase(Phase):
    count: int = 0

    def handle_start(self) -> Phase:
        self.__warn("Start")
        return self

    def handle_station_batch(self, batch: RawLines) -> Phase:
        self.__warn("Station")
        return self

    def handle_weather_batch(self, batch: RawLines) -> Phase:
        self.__warn("Weather")
        return self

    def handle_trip_batch(self, batch: RawLines) -> Phase:
        self._send_parsed(batch, parse_trip)
        self.count += len(batch.lines)
        return self

    def handle_end(self) -> Phase:
        logging.info(
            f"Job {self.job_id} | Received End, waiting for all batchs to be processed"
        )
        self.comms.set_all_batchs_done_callback(self.job_id, self._all_batchs_done)
        return self

    def _all_batchs_done(self) -> None:
        logging.info(
            f"Job {self.job_id} | Finished parsing all trips. Total processed in this"
            f" node: {self.count}"
        )
        self._send(End())
        self.comms.stop_consuming_job(self.job_id)
        self.on_finish(self)

    def __warn(self, name: str) -> None:
        logging.warn(
            f"Job {self.job_id} | Unexpected {name} received while receiving trips"
        )
