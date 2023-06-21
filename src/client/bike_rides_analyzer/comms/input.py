import logging
from typing import Iterable
from threading import Event
import zmq

from shared.messages import RecordStart, LinesBatch, RecordType

from . import Comms
from .socket import ClientSocket
from ..config import BikeRidesAnalyzerConfig


class CommsInput(Comms):
    batch_size: int

    def __init__(
        self,
        job_id: str,
        context: zmq.Context[zmq.Socket[None]],
        config: BikeRidesAnalyzerConfig,
        interrupt_event: Event,
    ) -> None:
        socket = ClientSocket(context, config.input_address, interrupt_event)
        super().__init__(socket, job_id)
        self.batch_size = config.batch_size

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
