from abc import ABC

from shared.serde import serialize
from shared.messages import ClientPayloads, Message

from .socket import ClientSocket


class Comms(ABC):
    job_id: str
    socket: ClientSocket

    def __init__(self, socket: ClientSocket, job_id: str) -> None:
        self.socket = socket
        self.job_id = job_id

    def send(self, payload: ClientPayloads) -> None:
        self.socket.send(serialize(Message(self.job_id, payload)))

    def close(self) -> None:
        self.socket.close()
