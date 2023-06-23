import logging
from threading import Event, Thread
import zmq

from shared.serde import deserialize, serialize
from shared.messages import (
    GetStat,
    Message as ClientMessage,
    NotAvailable,
    ServerMessages,
)

from .config import Config
from .stats import StatsStorage

CHECK_STOP_MS = 1000


class ClientHandler(Thread):
    context: zmq.Context[zmq.Socket[None]]
    stats: StatsStorage
    config: Config
    stop_event: Event

    def __init__(
        self,
        config: Config,
        context: zmq.Context[zmq.Socket[None]],
        stats: StatsStorage,
    ) -> None:
        """
        Initializes the client handler. Must be called from the main thread.
        """
        super().__init__()
        self.stats = stats
        self.context = context
        self.config = config
        self.stop_event = Event()

    def stop(self) -> None:
        """
        Stops a running server
        """
        self.stop_event.set()

    def run(self) -> None:
        logging.info("Client handler started")
        ClientHandlerInternal(
            self.context,
            self.config,
            self.stats,
            self.stop_event,
        ).run()


class ClientHandlerInternal:
    """
    Parts of the client handler that can be executed in a separate thread
    """

    clients_socket: zmq.Socket[None]
    stats: StatsStorage
    stop_event: Event

    def __init__(
        self,
        context: zmq.Context[zmq.Socket[None]],
        config: Config,
        stats: StatsStorage,
        stop_event: Event,
    ) -> None:
        self.clients_socket = context.socket(zmq.REP)
        self.clients_socket.bind(config.address)
        self.stop_event = stop_event
        self.stats = stats

    def run(self) -> None:
        poll_result = 0
        while not self.stop_event.is_set():
            if poll_result & zmq.POLLIN != 0:
                self.__handle_client()
            poll_result = self.clients_socket.poll(CHECK_STOP_MS, zmq.POLLIN)

        self.clients_socket.close()

    def __handle_client(self) -> None:
        """
        Handles a client request, either by sending him the stat he
        requested or by adding him to the list of clients waiting for that stat
        """
        msg: ClientMessage[GetStat] = deserialize(
            ClientMessage[GetStat], self.clients_socket.recv_string()
        )
        stat = self.stats.get(msg.job_id, msg.payload.stat_type)
        log = f"Job {msg.job_id} | Received request for stat {msg.payload.stat_type}"
        if stat is None:
            logging.debug(f"{log} | Stat not available - sending response")
            self.__send(NotAvailable())
        else:
            logging.info(f"{log} | Sending available stat")
            self.__send(stat)

    def __send(self, message: ServerMessages) -> None:
        """
        Sends a response to a client
        """
        self.clients_socket.send_string(serialize(message))
