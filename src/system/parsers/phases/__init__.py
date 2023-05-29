import logging
from typing import Callable

from common.messages.basic import BasicRecord
from common.messages.raw import RawBatch, RawRecord

from .parse import get_indexes, get_rows
from ..comms import SystemCommunication


class Phase:
    comms: SystemCommunication

    def __init__(self, comms: SystemCommunication) -> None:
        self.comms = comms

    def handle_station_batch(self, batch: RawBatch) -> "Phase":
        raise NotImplementedError()

    def handle_weather_batch(self, batch: RawBatch) -> "Phase":
        raise NotImplementedError()

    def handle_trip_batch(self, batch: RawBatch) -> "Phase":
        raise NotImplementedError()

    def handle_end(self) -> "Phase":
        raise NotImplementedError()

    def handle_record(self, raw_record: RawRecord) -> "Phase":
        return raw_record.be_handled_by(self)

    def _send_parsed(
        self,
        batch: RawBatch,
        parse_func: Callable[[list[str], dict[str, int], str], BasicRecord],
    ) -> None:
        indexes = get_indexes(batch)
        rows = get_rows(batch)
        for row in rows:
            if self.comms.is_stopped():
                logging.debug("Parser was stopped, skipping remaining records")
                break
            self.comms.send(parse_func(row, indexes, batch.city))
