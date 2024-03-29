import logging
from typing import Iterable
from threading import Event
import zmq
import time

from shared.messages import (
    RecordStart,
    LinesBatch,
    RecordType,
    ServerMessagesInput,
    Ack,
    NewJob,
    ClientJobId,
    NotAvailable,
)
from shared.serde import deserialize, serialize

from . import Comms
from .socket import ClientSocket
from ..config import BikeRidesAnalyzerConfig

RETRY_START_JOB_SECONDS = 10


class CommsInput(Comms):
    batch_size: int

    def __init__(
        self,
        context: zmq.Context[zmq.Socket[None]],
        config: BikeRidesAnalyzerConfig,
        interrupt_event: Event,
    ) -> None:
        socket = ClientSocket(context, config.input_address, interrupt_event)
        super().__init__(socket, None)
        self.batch_size = config.batch_size

    def start_job(self, identity: str) -> str:
        self.socket.send(serialize(NewJob(identity)))
        resp = self.recv()
        while isinstance(resp, NotAvailable):
            logging.warning("New job not available, retrying in 10 seconds")
            time.sleep(RETRY_START_JOB_SECONDS)
            self.socket.send(serialize(NewJob(identity)))
            resp = self.recv()
        if not isinstance(resp, ClientJobId):
            raise ValueError(f"Expected ClientJobId, got {type(resp)}")
        self.job_id = resp.job_id
        return resp.job_id

    def recv(self) -> ServerMessagesInput:
        return deserialize(ServerMessagesInput, self.socket.recv())

    def send_batchs(
        self, city: str, all_lines: Iterable[str], record_type: RecordType
    ) -> int:
        count = 0
        batch_number = 0
        csv_header, *data = all_lines

        start = RecordStart(record_type, city, csv_header)
        logging.debug(f"Sending RecordStart - {city=} - {record_type=}")
        self.send(start)
        logging.debug("Waiting for RecordStart ACK")
        self.recv_ack()

        logging.debug("Sending batchs")
        for batch in self.__batch(data):
            lines = LinesBatch(batch_number, batch)
            self.send(lines)
            self.recv_ack(batch_number)
            count += len(batch)
            batch_number += 1

        logging.info(f"{record_type} | {city} | Sent {count} {record_type} records")
        return count

    def __batch(self, lines: Iterable[str]) -> Iterable[list[str]]:
        batch = []
        for line in lines:
            batch.append(line)
            if len(batch) == self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def recv_ack(self, batch_number: int | None = None) -> None:
        response = self.recv()

        if not isinstance(response, Ack) or (
            batch_number is not None and response.batch_number != batch_number
        ):
            raise RuntimeError(
                f"Expected Ack with number {batch_number} but received response"
                f" {response}"
            )
