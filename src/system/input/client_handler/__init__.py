import logging
from dataclasses import dataclass
from typing import Callable
from functools import singledispatchmethod

from shared.messages import (
    ClientPayloads,
    RecordStart,
    LinesBatch,
    AllSent,
    ServerMessagesInput,
    Ack,
    Error,
    RecordType as RecordTypeBase,
)
from shared.socket import SocketStopWrapper

from common.messages import RecordType, End, Start
from common.messages.raw import RawLines
from common.persistence import StatePersistor, WithState

from ..comms import SystemCommunication
from .phase import Phase

TIMEOUT_MILLISECONDS = 1000


@dataclass
class State:
    phase: Phase
    # (record_type, city) -> latest batch number
    latest_batchs: dict[tuple[RecordTypeBase, str], int]
    latest_start: RecordStart | None = None
    all_sent: bool = False


class ClientHandler(WithState[State]):
    job_id: str
    comms: SystemCommunication
    socket: SocketStopWrapper
    on_finish: Callable[[str], None]

    def __init__(
        self,
        job_id: str,
        comms: SystemCommunication,
        socket: SocketStopWrapper,
        on_finish: Callable[[str], None],
    ) -> None:
        super().__init__(State(Phase(job_id), {}))
        self.job_id = job_id
        self.comms = comms
        self.socket = socket
        self.restore_from(job_id)
        self.on_finish = on_finish
        if not self.state.all_sent:
            comms.setup_job_queue(job_id)
            self.comms.send_msg(self.job_id, Start())
            self.comms.flush()

    @singledispatchmethod
    def handle_message(self, msg: ClientPayloads) -> ServerMessagesInput:
        raise RuntimeError(f"Invalid message type: {type(msg)}")

    @handle_message.register
    def handle_start(self, msg: RecordStart) -> ServerMessagesInput:
        if not self.state.phase.validate_phase(msg):
            return Error("Invalid phase for this RecordStart")
        self.latest_start = msg
        self.state.latest_batchs[(msg.record_type, msg.city)] = -1
        logging.info(
            f"Job {self.job_id} | Receiving {msg.record_type}s from {msg.city}"
        )
        self.store_to(self.job_id)
        return Ack()

    @handle_message.register
    def handle_batch(self, msg: LinesBatch) -> ServerMessagesInput:
        if self.latest_start is None:
            logging.warn(
                f"Job {self.job_id} | Received a batch of lines without its start"
            )
            return Error("Received a batch of lines without its start")
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
        self.store_to(self.job_id)
        return Ack(msg.batch_number)

    @handle_message.register
    def handle_all_sent(self, all_sent: AllSent) -> ServerMessagesInput:
        self.comms.send_msg(self.job_id, End())
        StatePersistor().remove(self.job_id)
        self.state.all_sent = True
        self.on_finish(self.job_id)
        logging.info(f"Job {self.job_id} | Finished sending input data")
        return Ack()

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
        return f"{self.latest_start.city};{batch_number}"
