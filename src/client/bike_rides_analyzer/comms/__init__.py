from abc import ABC

from shared.serde import serialize, deserialize
from shared.messages import Ack, ClientPayloads, ServerMessages, Message

from shared.socket import SocketStopWrapper


class Comms(ABC):
    job_id: str
    socket: SocketStopWrapper

    def send(self, payload: ClientPayloads) -> None:
        self.socket.send(serialize(Message(self.job_id, payload)))

    def recv(self) -> ServerMessages:
        return deserialize(ServerMessages, self.socket.recv())

    def close(self) -> None:
        self.socket.close()

    def recv_ack(self, batch_id: int | None = None) -> None:
        ack = self.recv()
        if not isinstance(ack, Ack) or (
            batch_id is not None and ack.batch_id != batch_id
        ):
            raise RuntimeError("Did not receive ACK for this message")
