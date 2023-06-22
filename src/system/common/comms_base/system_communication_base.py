import logging

from pika import BlockingConnection, ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel

from .protocol import CommsProtocol
from common.config_base import ConfigProtocol
from .util import get_host_data


# Base communication class. See protocol.py for more details about the methods.
class SystemCommunicationBase(CommsProtocol):
    __conn: BlockingConnection
    __ch: BlockingChannel
    __data: tuple[str, str] | None = None

    @property
    def connection(self) -> BlockingConnection:
        return self.__conn

    @property
    def channel(self) -> BlockingChannel:
        return self.__ch

    @property
    def id(self) -> str:
        if self.__data is None:
            self.__data = get_host_data()
        return self.__data[1]

    @property
    def name(self) -> str:
        if self.__data is None:
            self.__data = get_host_data()
        return self.__data[0]

    def __init__(self, config: ConfigProtocol) -> None:
        logging.info(f"Host ID: {self.id}")
        self.__conn = BlockingConnection(ConnectionParameters(host=config.rabbit_host))
        self.__ch = self.connection.channel()

    def reset_channel(self) -> None:
        self.__ch = self.connection.channel()

    def close(self) -> None:
        self.channel.close()
        self.connection.close()
