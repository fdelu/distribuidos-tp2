import os
from uuid import uuid4
from pika import BlockingConnection, ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel

from common.config_base import ConfigProtocol

from .protocol import CommsProtocol

from .send import CommsSend
from .send.reliable import ReliableSend
from .receive import CommsReceive
from .receive.reliable import ReliableReceive
from .util import setup_job_queues

__all__ = [
    "CommsProtocol",
    "CommsSend",
    "CommsReceive",
    "ReliableSend",
    "ReliableReceive",
    "setup_job_queues",
]

ID_FILE_PATH = "/host_id.txt"


# Base communication class. See protocol.py for more details about the methods.
class SystemCommunicationBase(CommsProtocol):
    __conn: BlockingConnection
    __ch: BlockingChannel
    __id: str

    @property
    def connection(self) -> BlockingConnection:
        return self.__conn

    @property
    def channel(self) -> BlockingChannel:
        return self.__ch

    @property
    def id(self) -> str:
        return self.__id

    def __init__(self, config: ConfigProtocol) -> None:
        self.__setup_id()
        self.__conn = BlockingConnection(ConnectionParameters(host=config.rabbit_host))
        self.__ch = self.connection.channel()

    def __setup_id(self) -> None:
        if os.path.exists(ID_FILE_PATH):
            with open(ID_FILE_PATH, "r") as f:
                self.__id = f.read()
            return

        self.__id = str(uuid4())
        with open(ID_FILE_PATH, "w") as f:
            f.write(self.id)

    def reset_channel(self) -> None:
        self.__ch = self.connection.channel()

    def close(self) -> None:
        self.channel.close()
        self.connection.close()
