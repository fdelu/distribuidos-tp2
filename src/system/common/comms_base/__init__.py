import logging
from pika import BlockingConnection, ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel

from common.config_base import ConfigProtocol

from .protocol import CommsProtocol

from .send import CommsSend
from .send.reliable import ReliableSend
from .receive import CommsReceive
from .receive.reliable import ReliableReceive
from .util import setup_job_queues, get_host_id

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
    __id: str | None = None

    @property
    def connection(self) -> BlockingConnection:
        return self.__conn

    @property
    def channel(self) -> BlockingChannel:
        return self.__ch

    @property
    def id(self) -> str:
        if self.__id is None:
            self.__id = get_host_id()
        return self.__id

    def __init__(self, config: ConfigProtocol) -> None:
        logging.info(f"Host ID: {self.id}")
        self.__conn = BlockingConnection(ConnectionParameters(host=config.rabbit_host))
        self.__ch = self.connection.channel()

    def reset_channel(self) -> None:
        self.__ch = self.connection.channel()

    def close(self) -> None:
        self.channel.close()
        self.connection.close()
