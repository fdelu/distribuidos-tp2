import logging
from typing import Iterable
from threading import Event
import zmq

from shared.messages import RecordStart, LinesBatch, RecordType
from shared.socket import SocketStopWrapper

from . import Comms
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
        super().__init__()
        self.job_id = job_id
        socket = context.socket(zmq.REQ)
        socket.connect(config.input_address)
        self.socket = SocketStopWrapper(socket, interrupt_event)
        self.batch_size = config.batch_size

    def send_batchs(
        self, city: str, all_lines: Iterable[str], record_type: RecordType
    ) -> int:
        count = 0
        batch_id = 0
        csv_header, *data = all_lines

        start = RecordStart(record_type, city, csv_header)
        self.send(start)
        self.recv_ack()

        for batch in self.__batch(data):
            lines = LinesBatch(batch_id, batch)
            self.send(lines)
            self.recv_ack(batch_id)
            count += len(batch)
            batch_id += 1

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
