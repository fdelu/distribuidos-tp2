import logging
import signal
from threading import Event
import zmq
from enum import StrEnum
from hashlib import sha256

from shared.socket import SocketStopWrapper
from shared.messages import SplitChar, RecordType as BaseRecordType

from common.messages import RecordType, End, Message, Batch
from common.messages.raw import RawLines, RawRecord

from .config import Config
from .comms import SystemCommunication


TIMEOUT_MILLISECONDS = 1000


class Phase(StrEnum):
    StationsWeather = "stations_weather"
    Trips = "trips"
    End = "end"


class ClientHandler:
    config: Config
    phase: Phase

    socket: SocketStopWrapper

    stop_event: Event

    def __init__(self, config: Config, context: zmq.Context[zmq.Socket[None]]) -> None:
        self.config = config
        self.phase = Phase.StationsWeather
        self.comms = SystemCommunication(config)
        self.stop_event = Event()

        socket = context.socket(zmq.PAIR)
        socket.bind(config.address)
        self.socket = SocketStopWrapper(socket, self.stop_event)

    def run(self) -> None:
        self.setup_interrupt()
        logging.info("Receiving weather & stations")
        try:
            self.receive_data()
        except (InterruptedError, KeyboardInterrupt):
            logging.info("Input interrupted by user")
        logging.info("Finished receiving data")
        self.comms.close()
        self.socket.close()

    def setup_interrupt(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop_event.set())

    def receive_data(self) -> None:
        while not self.stop_event.is_set():
            msg = self.socket.recv()

            record_type, city = self.__get_record_data(msg)
            if not self.__validate_phase(record_type):
                continue

            if self.phase == Phase.StationsWeather and record_type == RecordType.TRIP:
                self.phase = Phase.Trips
                logging.info(f"{self.phase} | Sending trips")

            if record_type == RecordType.END:
                self.__handle_end()
            else:
                self.__handle_batch(record_type, city)

    def __get_record_data(self, header: str) -> tuple[RecordType, str]:
        if header == RecordType.END:
            return RecordType.END, ""
        record_type, city = header.split(SplitChar.HEADER)
        return RecordType(record_type), city

    def __handle_batch(self, record_type: RecordType, city: str) -> None:
        count = 0
        msg = self.socket.recv()
        headers, data = msg.split(SplitChar.RECORDS, 1)
        while msg != RecordType.END:
            raw = RawLines(record_type, city, headers, data.splitlines())
            hash = sha256(data.encode()).hexdigest()
            self._send(self.__build_msg(raw, hash, "123"))
            count += len(msg.splitlines())
            msg = self.socket.recv()
        self.socket.send(BaseRecordType.ACK)
        logging.info(
            f"{self.phase} | {city} | Received and sent {count} {record_type} records"
        )

    def __build_msg(
        self, lines: RawLines, hash: str, job_id: str
    ) -> Batch[Message[RawRecord]]:
        return Batch([Message(job_id, lines)], hash)

    def __handle_end(self) -> None:
        self._send(Batch([Message("123", End())], None))
        self.stop_event.set()

    def __validate_phase(self, record_type: RecordType) -> bool:
        if (
            self.phase == Phase.Trips
            and record_type not in (RecordType.TRIP, RecordType.END)
        ) or self.phase == Phase.End:
            logging.error(
                f"Received {record_type} record in invalid phase {self.phase}"
            )
            return False
        return True

    def _send(self, record: Batch[Message[RawRecord]]) -> None:
        self.comms.send(record)
