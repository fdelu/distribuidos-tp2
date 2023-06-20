from dataclasses import dataclass
import logging
from typing import Callable

from common.messages import Message, End, Start
from common.messages.basic import BasicRecord
from common.messages.raw import RawLines, RawRecord

from common.persistence import WithState, StatePersistor

from .parse import get_indexes, get_rows, parse_trip, parse_station, parse_weather
from .comms import SystemCommunication


@dataclass
class State:
    count: int = 0
    receiving_trips: bool = False


class JobParser(WithState[State]):
    comms: SystemCommunication
    job_id: str
    on_finish: Callable[["JobParser"], None]

    def __init__(
        self,
        comms: SystemCommunication,
        job_id: str,
        on_finish: Callable[["JobParser"], None],
    ) -> None:
        super().__init__(State())
        self.comms = comms
        self.job_id = job_id
        self.on_finish = on_finish

    def restore_state(self) -> None:
        self.restore_from(self.job_id)

    def store_state(self) -> None:
        self.store_to(self.job_id)

    def handle_start(self, start: Start) -> None:
        self.comms.start_consuming_job(self.job_id)

    def handle_station_batch(self, batch: RawLines) -> None:
        self.__send_parsed(batch, parse_station)

    def handle_weather_batch(self, batch: RawLines) -> None:
        self.__send_parsed(batch, parse_weather)

    def handle_trip_batch(self, batch: RawLines) -> None:
        if not self.state.receiving_trips:
            logging.info(f"Job {self.job_id} | Finished parsing weather & stations")
            self.state.receiving_trips = True
            self.__send_start()

        self.__send_parsed(batch, parse_trip)
        self.state.count += len(batch.lines)

    def handle_end(self, end: End) -> None:
        logging.info(
            f"Job {self.job_id} | Received End, waiting for all batchs to be processed"
        )
        self.comms.set_all_batchs_done_callback(self.job_id, self.__finished)

    def handle_record(self, raw_record: RawRecord) -> None:
        return raw_record.be_handled_by(self)

    def __send_parsed(
        self,
        batch: RawLines,
        parse_func: Callable[[list[str], dict[str, int], str], BasicRecord],
    ) -> None:
        indexes = get_indexes(batch)
        rows = get_rows(batch)
        for row in rows:
            if self.comms.is_stopped():
                logging.debug(
                    f"Job {self.job_id} | Parser was stopped, skipping remaining"
                    " records"
                )
                break
            msg = Message(self.job_id, parse_func(row, indexes, batch.city))
            self.comms.send(msg)

    def __finished(self) -> None:
        if not self.state.receiving_trips:
            self.state.receiving_trips = True
            self.__send_start()

        logging.info(
            f"Job {self.job_id} | Finished parsing. Total trips processed in this"
            f" node: {self.state.count}"
        )
        self.comms.stop_consuming_job(self.job_id)
        StatePersistor().remove(self.job_id)
        self.comms.send(Message(self.job_id, End(self.comms.id)))
        self.on_finish(self)

    def __send_start(self) -> None:
        self.comms.send(Message(self.job_id, Start(self.comms.id)), force_msg_id=None)
