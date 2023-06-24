import logging
import signal
from threading import Event
import zmq

from shared.serde import deserialize, serialize
from shared.socket import SocketStopWrapper
from shared.messages import ServerMessagesInput, Message, ClientPayloadsInput
from common.job_tracker import JobTracker

from .client_handler import ClientHandler
from .config import Config
from .comms import SystemCommunication

TIMEOUT_MILLISECONDS = 200


class InputServer:
    comms: SystemCommunication
    context: zmq.Context[zmq.Socket[None]]
    socket: SocketStopWrapper

    handlers: dict[str, ClientHandler]
    stop_event: Event
    tracker: JobTracker

    def __init__(self) -> None:
        self.comms = SystemCommunication()
        self.stop_event = Event()
        self.handlers = {}

        self.context = zmq.Context[zmq.Socket[None]]()
        self.context.setsockopt(zmq.LINGER, 0)  # Don't block on close

        socket = self.context.socket(zmq.REP)
        socket.bind(Config().address)
        self.socket = SocketStopWrapper(socket, self.stop_event)

        self.job_tracker = JobTracker()
        self.job_tracker.restore(self.handlers, self.__handler)

    def finished(self, job_id: str) -> None:
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
                res = self.receive_data(deserialize(Message[ClientPayloadsInput], req))
                self.comms.flush()
            except TimeoutError:
                pass

    def receive_data(self, msg: Message[ClientPayloadsInput]) -> ServerMessagesInput:
        handler = self.handlers.get(msg.job_id, None)
        if handler is None:
            logging.info(f"Starting job {msg.job_id}")
            self.job_tracker.start_job(msg.job_id)
            handler = self.__handler(msg.job_id)
        self.handlers[msg.job_id] = handler
        return handler.handle_message(msg.payload)

    def __handler(self, job_id: str) -> ClientHandler:
        return ClientHandler(job_id, self.comms, self.socket, self.finished)

    def setup_interrupt(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop_event.set())

    def run(self) -> None:
        self.comms.start()
        self.setup_interrupt()
        logging.info("Starting to receive requests")
        try:
            self.receive_loop()
        except (InterruptedError, KeyboardInterrupt):
            logging.info("Input interrupted by user")
        logging.info("Finished receiving data")

    def cleanup(self) -> None:
        self.comms.stop()
        self.comms.close()
        self.socket.close()
        self.context.term()
