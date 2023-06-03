import logging
from typing import Callable

from common.messages import Message
from common.messages.basic import BasicRecord
from common.messages.raw import RawLines, RawRecord

from .parse import get_indexes, get_rows
from ..comms import SystemCommunication


class Phase:
    comms: SystemCommunication
    job_id: str
    on_finish: Callable[["Phase"], None]

    def __init__(
        self,
        comms: SystemCommunication,
        job_id: str,
        on_finish: Callable[["Phase"], None],
    ) -> None:
        self.comms = comms
        self.job_id = job_id
        self.on_finish = on_finish

    def handle_station_batch(self, batch: RawLines) -> "Phase":
        raise NotImplementedError()

    def handle_weather_batch(self, batch: RawLines) -> "Phase":
        raise NotImplementedError()

    def handle_trip_batch(self, batch: RawLines) -> "Phase":
        raise NotImplementedError()

    def handle_end(self) -> "Phase":
        raise NotImplementedError()

    def handle_record(self, raw_record: RawRecord) -> "Phase":
        return raw_record.be_handled_by(self)

    def _send_parsed(
        self,
        batch: RawLines,
        parse_func: Callable[[list[str], dict[str, int], str], BasicRecord],
    ) -> None:
        indexes = get_indexes(batch)
        rows = get_rows(batch)
        for row in rows:
            if self.comms.is_stopped():
                logging.debug("Parser was stopped, skipping remaining records")
                break
            self._send(parse_func(row, indexes, batch.city))

    def _send(self, record: BasicRecord) -> None:
        self.comms.send(Message(self.job_id, record))
