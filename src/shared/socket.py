import logging
from threading import Event
import zmq

TIMEOUT_MILLISECONDS = 1000


class SocketStopWrapper:
    socket: zmq.Socket[None]
    in_poller: zmq.Poller
    out_poller: zmq.Poller
    stop_event: Event

    def __init__(self, socket: zmq.Socket[None], stop_event: Event) -> None:
        self.socket = socket
        self.in_poller = zmq.Poller()
        self.in_poller.register(self.socket, zmq.POLLIN)
        self.out_poller = zmq.Poller()
        self.out_poller.register(self.socket, zmq.POLLOUT)
        self.stop_event = stop_event

    def send(self, data: str) -> None:
        sock = self.__wait_until_ready(self.out_poller)
        sock.send_string(data)

    def recv(self) -> str:
        sock = self.__wait_until_ready(self.in_poller)
        return sock.recv_string()

    def __wait_until_ready(self, poller: zmq.Poller) -> zmq.Socket[None]:
        self.__check_stop()
        ready = poller.poll(TIMEOUT_MILLISECONDS)
        while len(ready) == 0:
            self.__check_stop()
            ready = poller.poll(TIMEOUT_MILLISECONDS)
        return ready[0][0]

    def __check_stop(self) -> None:
        if self.stop_event.is_set():
            self.stop_event.clear()
            logging.info("Canceling socket operation")
            raise InterruptedError()

    def close(self) -> None:
        self.in_poller.unregister(self.socket)
        self.out_poller.unregister(self.socket)
        self.socket.close()
