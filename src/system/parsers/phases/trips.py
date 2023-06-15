import logging

from common.messages import End
from common.messages.raw import RawLines

from . import Phase
from .parse import parse_trip


class TripsPhase(Phase):
    count: int = 0

    def handle_station_batch(self, batch: RawLines) -> Phase:
        logging.warn("Unexpected Station received while receiving trips")
        return self

    def handle_weather_batch(self, batch: RawLines) -> Phase:
        logging.warn("Unexpected Weather received while receiving trips")
        return self

    def handle_trip_batch(self, batch: RawLines) -> Phase:
        self._send_parsed(batch, parse_trip)
        self.count += len(batch.lines)
        return self

    def handle_end(self) -> Phase:
        logging.info("Received End, waiting for all batchs to be processed")
        self.comms.set_all_batchs_done_callback(self.job_id, self._all_batchs_done)
        return self

    def _all_batchs_done(self) -> None:
        logging.info(
            f"Finished parsing all trips. Total processed in this node: {self.count}"
        )
        self._send(End())
        self.comms.stop_consuming_job(self.job_id)
        self.on_finish(self)
