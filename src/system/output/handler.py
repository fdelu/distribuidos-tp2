import json
import logging
from threading import Thread
from typing import Any
import zmq

from shared.messages import StatType

from .config import Config
from .stats import StatsStorage

CONTROL_ADDR = "inproc://control"
END_MSG = "END"


class ClientHandler(Thread):
    context: zmq.Context[zmq.Socket[None]]
    stats: StatsStorage
    config: Config

    control_socket: zmq.Socket[None] | None = None

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

        self.control_socket = None

    def run(self) -> None:
        logging.info("Client handler started")
        ClientHandlerInternal(self.context, self.config, self.stats).run()

    def stop(self) -> None:
        """
        Stops a running server
        Must be called from the main thread.
        """
        if self.control_socket is None:
            self.control_socket = self.__connect_control()
        self.control_socket.send_string(END_MSG)
        self.control_socket.close()

    def received(self, job_id: str, type: StatType) -> None:
        """
        Executed when a stat is received.
        Must be called from the main thread.
        """
        if self.control_socket is None:
            self.control_socket = self.__connect_control()
        self.control_socket.send_string(type)

    def __connect_control(self) -> zmq.Socket[None]:
        socket = self.context.socket(zmq.PAIR)
        socket.connect(CONTROL_ADDR)
        return socket


class ClientHandlerInternal:
    """
    Parts of the client handler that can be executed in a separate thread
    """

    clients_socket: zmq.Socket[None]
    control_socket: zmq.Socket[None]

    stats: StatsStorage
    pending_clients: dict[StatType, list[bytes]] = {}  # stat_type -> clients waiting

    def __init__(
        self,
        context: zmq.Context[zmq.Socket[None]],
        config: Config,
        stats: StatsStorage,
    ) -> None:
        self.clients_socket = context.socket(zmq.ROUTER)
        self.clients_socket.bind(config.address)
        self.control_socket = context.socket(zmq.PAIR)
        self.control_socket.bind(CONTROL_ADDR)

        self.stats = stats

    def run(self) -> None:
        poller = zmq.Poller()
        poller.register(self.clients_socket, zmq.POLLIN)
        poller.register(self.control_socket, zmq.POLLIN)

        stop = False
        while not stop:
            stop = self.__receive(poller)

        poller.unregister(self.clients_socket)
        poller.unregister(self.control_socket)
        self.clients_socket.close()
        self.control_socket.close()

    def __receive(self, poller: zmq.Poller) -> bool:
        """
        Receives a message. Returns True if the server should stop.
        """
        ready = [x[0] for x in poller.poll()]
        if self.control_socket in ready:
            msg = self.control_socket.recv_string()
            if msg == END_MSG:
                return True
            self.__handle_received(StatType(msg))

        if self.clients_socket in ready:
            id, _, body = self.clients_socket.recv_multipart()
            self.__handle_client(id, body)
        return False

    def __handle_client(self, id: bytes, msg: bytes) -> None:
        """
        Handles a client request, either by sending him the stat he
        requested or by adding him to the list of clients waiting for that stat
        """
        msg_str = msg.decode()
        logging.info(f"Received request for stats {msg_str}")
        try:
            type = StatType(msg_str)
        except ValueError:
            logging.warning(f"Invalid stat type was requested: {msg_str}")
            self.clients_socket.send_multipart([id, b"", b"Invalid stat type"])
            return
        stat = self.stats.get("123", type)
        if stat is None:
            self.pending_clients.setdefault(type, []).append(id)
        else:
            self.__send_stat(id, stat)

    def __handle_received(self, type: StatType) -> None:
        """
        Sends a stat to all clients waiting for it when it is received
        """
        waiting = self.pending_clients.pop(type, [])
        logging.info(f"Sending received stat '{type}' to {len(waiting)} clients")
        stat = self.stats.get("123", type)
        if stat is None:
            raise RuntimeError("Stat was received but it is not available")
        for id in waiting:
            self.__send_stat(id, stat)

    def __send_stat(self, id: bytes, stat: Any) -> None:
        """
        Sends a stat to a client
        """
        logging.debug("Sending response to client")
        self.clients_socket.send_multipart([id, b"", json.dumps(stat).encode()])
