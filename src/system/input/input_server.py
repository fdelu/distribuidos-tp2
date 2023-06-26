import logging
import signal
from threading import Event
from uuid import uuid4
import zmq

from shared.serde import deserialize, serialize
from shared.socket import SocketStopWrapper
from shared.messages import (
    ServerMessagesInput,
    Message,
    ClientPayloadsInput,
    NewJob,
    NotAvailable,
    Error,
    ClientJobId,
    Ack,
    AllSent,
)
from common.job_tracker import JobTracker

from .client_handler import ClientHandler
from .config import Config
from .comms import SystemCommunication

TIMEOUT_MILLISECONDS = 200


class InputServer:
    comms: SystemCommunication
    socket: SocketStopWrapper

    handlers: dict[str, ClientHandler]
    stop_event: Event
    tracker: JobTracker

    def __init__(
        self, context: zmq.Context[zmq.Socket[None]], comms: SystemCommunication
    ) -> None:
        self.comms = comms
        self.stop_event = Event()
        self.handlers = {}
        socket = context.socket(zmq.REP)
        socket.bind(Config().address)
        self.socket = SocketStopWrapper(socket, self.stop_event)
        self.job_tracker = JobTracker()

    def finished(self, job_id: str) -> None:
        logging.info(f"Finished job {job_id}")
        self.job_tracker.finished_job(job_id)
        self.handlers.pop(job_id)

    def receive_loop(self) -> None:
        res = None
        while not self.stop_event.is_set():
            try:
                if res is not None:
                    self.socket.send(serialize(res), timeout_ms=TIMEOUT_MILLISECONDS)
                    res = None
                req = self.socket.recv(timeout_ms=TIMEOUT_MILLISECONDS)
                res = self.handle_request(
                    deserialize(Message[ClientPayloadsInput] | NewJob, req)
                )
                self.comms.flush()
            except TimeoutError:
                pass

    def handle_request(
        self, msg: Message[ClientPayloadsInput] | NewJob
    ) -> ServerMessagesInput:
        if isinstance(msg, NewJob):
            for existing_handler in self.handlers.values():
                if existing_handler.state.client_identity == msg.identity:
                    return ClientJobId(existing_handler.job_id)

            if len(self.job_tracker.state.in_progress) >= Config().max_jobs:
                return NotAvailable()

            job_id = self.__new_job()
            logging.info(f"Starting job {job_id}")
            self.job_tracker.start_job(job_id)
            self.handlers[job_id] = self.__handler(job_id, msg.identity)
            return ClientJobId(job_id)

        handler = self.handlers.get(msg.job_id, None)
        if handler is None:
            if isinstance(msg.payload, AllSent):
                # We probably went down and already finished the job
                return Ack()
            return Error(f"Job {msg.job_id} not found")

        return handler.handle_message(msg.payload)

    def __handler(self, job_id: str, client: str | None = None) -> ClientHandler:
        return ClientHandler(job_id, self.comms, self.socket, self.finished, client)

    def __new_job(self) -> str:
        job_id = str(uuid4())
        while job_id in self.job_tracker:
            job_id = str(uuid4())
        return job_id

    def setup_interrupt(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop_event.set())

    def run(self) -> None:
        self.setup_interrupt()
        logging.info("Starting to receive requests")
        try:
            self.job_tracker.restore(self.handlers, self.__handler)
            self.receive_loop()
        except (InterruptedError, KeyboardInterrupt):
            logging.info("Input interrupted by user")
        logging.info("Finished receiving data")

    def cleanup(self) -> None:
        self.socket.close()
