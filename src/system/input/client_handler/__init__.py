import logging
from dataclasses import dataclass

from shared.serde import serialize
from shared.messages import RecordStart, Ack, LinesBatch, RecordType as RecordTypeBase
from shared.socket import SocketStopWrapper

from common.messages import RecordType, End, Start
from common.messages.raw import RawLines
from common.persistence import StatePersistor

from ..config import Config
from ..comms import SystemCommunication
from .phase import Phase

TIMEOUT_MILLISECONDS = 1000


@dataclass
class State:
    phase: Phase
    # (record_type, city) -> latest batch number
    latest_batchs: dict[tuple[RecordTypeBase, str], int]
    latest_start: RecordStart | None = None


class ClientHandler:
    job_id: str
    config: Config
    comms: SystemCommunication
    socket: SocketStopWrapper
    state: State

    def __init__(
        self,
        job_id: str,
        config: Config,
        comms: SystemCommunication,
        socket: SocketStopWrapper,
    ) -> None:
        self.job_id = job_id
        self.config = config
        self.comms = comms
        self.socket = socket
        self.state = StatePersistor().load(job_id, State) or State(
            Phase(self.job_id), {}
        )

        logging.info(f"Starting job {job_id} - Sending Start to parsers")
        comms.setup_job_queue(job_id)
        self.comms.send_msg(self.job_id, Start())
        self.comms.flush()

    def handle_start(self, msg: RecordStart) -> bool:
        if not self.state.phase.validate_phase(msg):
            return False
        self.latest_start = msg
        self.state.latest_batchs[(msg.record_type, msg.city)] = -1
        self.__save()
        self.__ack()
        return True

    def handle_batch(self, msg: LinesBatch) -> bool:
        if self.latest_start is None:
            logging.warn(
                f"Job {self.job_id} | Received a batch of lines without its start"
            )
            return False
        count = 0
        if not self.__already_processed(msg.batch_number):
            raw = RawLines(
                RecordType(self.latest_start.record_type),
                self.latest_start.city,
                self.latest_start.headers,
                msg.lines,
            )
            msg_id = self.__get_msg_id(msg.batch_number)
            count += len(msg.lines)
            self.__set_latest_batch(msg.batch_number)
            self.comms.send_msg(self.job_id, raw, msg_id)
        self.__save()
        self.__ack(msg.batch_number)
        return True

    def handle_all_sent(self) -> bool:
        self.comms.send_msg(self.job_id, End())
        StatePersistor().remove(self.job_id)
        self.comms.flush()
        self.__ack()
        logging.info(f"Job {self.job_id} | Finished sending input data")
        return False

    def __ack(self, batch_number: int | None = None) -> None:
        self.socket.send(serialize(Ack(batch_number)))

    def __save(self) -> None:
        StatePersistor().store(self.job_id, self.state)
        self.comms.flush()

    def __already_processed(self, batch_number: int) -> bool:
        if not self.latest_start:
            return False
        return (
            batch_number
            <= self.state.latest_batchs[
                (self.latest_start.record_type, self.latest_start.city)
            ]
        )

    def __set_latest_batch(self, batch_number: int) -> None:
        if not self.latest_start:
            return
        self.state.latest_batchs[
            (self.latest_start.record_type, self.latest_start.city)
        ] = batch_number

    def __get_msg_id(self, batch_number: int | str) -> str | None:
        if not self.latest_start or self.latest_start.record_type != RecordType.TRIP:
            return None
        return f"{self.job_id};{self.latest_start.city};{batch_number}"
