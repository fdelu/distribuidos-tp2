import logging

from shared.serde import serialize
from shared.messages import RecordStart, Ack, LinesBatch
from shared.socket import SocketStopWrapper

from common.messages import RecordType, Message, Package, End, Start
from common.messages.raw import RawLines, RawRecord

from ..config import Config
from ..comms import SystemCommunication
from .phase import Phase

TIMEOUT_MILLISECONDS = 1000


class ClientHandler:
    job_id: str
    config: Config
    phase: Phase
    last_start: RecordStart | None = None

    comms: SystemCommunication
    socket: SocketStopWrapper

    def __init__(
        self,
        job_id: str,
        config: Config,
        comms: SystemCommunication,
        socket: SocketStopWrapper,
    ) -> None:
        self.job_id = job_id
        self.config = config
        self.phase = Phase(job_id)
        self.comms = comms
        self.socket = socket

        logging.info(f"Starting job {job_id} - Sending Start to parsers")
        comms.setup_job_queue(job_id)
        self.__send(Start())

    def handle_start(self, msg: RecordStart) -> bool:
        if not self.phase.validate_phase(msg):
            return False
        self.last_start = msg
        self.socket.send(serialize(Ack()))
        return True

    def handle_batch(self, msg: LinesBatch) -> bool:
        if self.last_start is None:
            logging.warn(
                f"Job {self.job_id} | Received a batch of lines without its start"
            )
            return False
        count = 0

        raw = RawLines(
            RecordType(self.last_start.record_type),
            self.last_start.city,
            self.last_start.headers,
            msg.lines,
        )
        msg_id = self.__get_msg_id(msg.batch_id)
        self.__send(raw, msg_id)
        count += len(msg.lines)
        self.socket.send(serialize(Ack(msg.batch_id)))
        return True

    def handle_all_sent(self) -> bool:
        self.__send(End())
        self.socket.send(serialize(Ack()))
        logging.info(f"Job {self.job_id} | Finished sending input data")
        return False

    def __get_msg_id(self, batch_number: int | str) -> str | None:
        if not self.last_start or self.last_start.record_type != RecordType.TRIP:
            return None
        return f"{self.job_id};{self.last_start.city};{batch_number}"

    def __send(self, record: RawRecord, msg_id: str | None = None) -> None:
        package = Package([Message(self.job_id, record)], msg_id)
        self.comms.send(package)