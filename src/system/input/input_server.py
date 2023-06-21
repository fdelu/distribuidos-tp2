import logging
import signal
from threading import Event
import zmq

from shared.serde import deserialize
from shared.socket import SocketStopWrapper
from common.messages.input import Message

from .client_handler import ClientHandler
from .config import Config
from .comms import SystemCommunication


class InputServer:
    comms: SystemCommunication
    context: zmq.Context[zmq.Socket[None]]
    socket: SocketStopWrapper

    handlers: dict[str, ClientHandler]
    stop_event: Event
    config: Config

    def __init__(self, config: Config) -> None:
        self.comms = SystemCommunication(config)
        self.stop_event = Event()
        self.config = config
        self.handlers = {}

        self.context = zmq.Context[zmq.Socket[None]]()
        self.context.setsockopt(zmq.LINGER, 0)  # Don't block on close

        socket = self.context.socket(zmq.REP)
        socket.bind(config.address)
        self.socket = SocketStopWrapper(socket, self.stop_event)

    def receive_data(self) -> None:
        while not self.stop_event.is_set():
            msg: Message = deserialize(Message, self.socket.recv())
            handler = self.handlers.get(msg.job_id) or ClientHandler(
                msg.job_id, self.config, self.comms, self.socket
            )
            in_progress = msg.payload.be_handled_by(handler)
            self.handlers[msg.job_id] = handler

            if not in_progress:
                self.handlers.pop(msg.job_id)

    def setup_interrupt(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop_event.set())

    def run(self) -> None:
        self.setup_interrupt()
        logging.info("Starting to receive requests")
        try:
            self.receive_data()
        except (InterruptedError, KeyboardInterrupt):
            logging.info("Input interrupted by user")
        logging.info("Finished receiving data")
        self.comms.close()
        self.socket.close()
        self.context.term()
