import logging
from threading import Event
import zmq
import time

TIMEOUT_MILLISECONDS = 500


class SocketStopWrapper:
    socket: zmq.Socket[None]
    stop_event: Event
    last_sent: str | None

    def __init__(self, socket: zmq.Socket[None], stop_event: Event) -> None:
        self.socket = socket
        self.stop_event = stop_event
        self.last_sent = None

    def send(self, data: str, timeout_ms: float | None = None) -> None:
        self.__wait_until_ready(zmq.POLLOUT, timeout_ms=timeout_ms)
        self.socket.send_string(data)
        self.last_sent = data

    def recv(self, timeout_ms: float | None = None) -> str:
        self.__wait_until_ready(zmq.POLLIN, timeout_ms=timeout_ms)
        return self.socket.recv_string()

    def __wait_until_ready(self, flag: int, timeout_ms: float | None) -> None:
        start = time.time()
        self.__check_stop()
        while self.socket.poll(timeout=TIMEOUT_MILLISECONDS, flags=flag) & flag == 0:
            self.__check_stop()
            if timeout_ms is not None and (time.time() - start) * 1000 > timeout_ms:
                raise TimeoutError()

    def __check_stop(self) -> None:
        if self.stop_event.is_set():
            self.stop_event.clear()
            logging.info("Canceling socket operation")
            raise InterruptedError()

    def close(self) -> None:
        self.socket.close()
