from abc import ABC

from shared.serde import serialize, deserialize
from shared.messages import Ack, ClientPayloads, ServerMessages, Message

from .socket import ClientSocket


class Comms(ABC):
    job_id: str
    socket: ClientSocket

    def __init__(self, socket: ClientSocket, job_id: str) -> None:
        self.socket = socket
        self.job_id = job_id

    def send(self, payload: ClientPayloads) -> None:
        self.socket.send(serialize(Message(self.job_id, payload)))

    def recv(self) -> ServerMessages:
        return deserialize(ServerMessages, self.socket.recv())

    def close(self) -> None:
        self.socket.close()

    def recv_ack(self, batch_number: int | None = None) -> None:
        ack = self.recv()
        if not isinstance(ack, Ack) or (
            batch_number is not None and ack.batch_number != batch_number
        ):
            raise RuntimeError("Did not receive ACK for this message")
