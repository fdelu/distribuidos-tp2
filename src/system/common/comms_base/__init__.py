from pika import BlockingConnection, ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel

from common.config_base import ConfigProtocol

from .protocol import CommsProtocol

from .send import CommsSend
from .send.reliable import ReliableSend
from .receive import CommsReceive
from .receive.reliable import ReliableReceive

__all__ = [
    "CommsProtocol",
    "CommsSend",
    "CommsReceive",
    "ReliableSend",
    "ReliableReceive",
]


# Base communication class. See protocol.py for more details about the methods.
class SystemCommunicationBase(CommsProtocol):
    conn: BlockingConnection
    ch: BlockingChannel

    @property
    def connection(self) -> BlockingConnection:
        return self.conn

    @property
    def channel(self) -> BlockingChannel:
        return self.ch

    def __init__(self, config: ConfigProtocol) -> None:
        self.conn = BlockingConnection(ConnectionParameters(host=config.rabbit_host))
        self.ch = self.conn.channel()

    def close(self) -> None:
        """
        Closes the connection
        """
        self.ch.close()
        self.conn.close()
