from typing import Callable
import zmq
from threading import Event

from shared.socket import SocketStopWrapper

RESEND_AFTER_MS = 5000


class ClientSocket:
    inner: SocketStopWrapper
    last_sent: str | None
    __new_inner: Callable[[], None]

    def __init__(
        self, context: zmq.Context[zmq.Socket[None]], address: str, stop_event: Event
    ):
        def new_inner() -> None:
            socket = context.socket(zmq.REQ)
            socket.connect(address)
            self.inner = SocketStopWrapper(socket, stop_event)

        new_inner()
        self.__new_inner = new_inner
        self.last_sent = None

    def recv(self) -> str:
        while True:
            try:
                return self.inner.recv(timeout_ms=RESEND_AFTER_MS)
            except TimeoutError:
                # Reset socket and retry
                self.inner.close()
                self.__new_inner()

    def send(self, data: str) -> None:
        self.inner.send(data)
        self.last_sent = data

    def close(self) -> None:
        self.inner.close()
