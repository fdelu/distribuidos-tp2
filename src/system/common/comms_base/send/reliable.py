from abc import abstractmethod
from typing import Generic, TypeVar

from shared.serde import serialize

from common.messages import Package
from common.persistence.persistor import StatePersistor

from ..receive import ReceiveConfig
from ..receive.reliable import ReliableReceive
from ..base import SystemCommunicationBase
from ..protocol import OUT


LAST_SENT_KEY = "_last_sent"
IN = TypeVar("IN")


class _Unset:
    ...


class ReliableComms(ReliableReceive[IN], SystemCommunicationBase, Generic[IN, OUT]):
    """
    Comms with send batching capabilities. It will group messages in packages
    with the same routing key and send them when the batch being received is
    done.
    """

    last_messages: list[tuple[str, str, str]]  # exchange, routing key, message
    packages: dict[tuple[str, str], "Package[OUT]"]  # exchange, routing_key -> batch
    routing_count: int = 0

    def __init__(self, config: ReceiveConfig) -> None:
        super().__init__(config)
        self.packages = {}
        self.last_messages = (
            StatePersistor().load(LAST_SENT_KEY, list[tuple[str, str, str]]) or []
        )
        self.__send_messages()

    def send(self, record: OUT, force_msg_id: str | None | _Unset = _Unset()) -> None:
        key = self._get_routing_details(record)

        if not isinstance(force_msg_id, _Unset):
            package = Package([record], force_msg_id)
        else:
            package = self.packages.get(key) or Package([], self.__next_message_id())
            package.messages.append(record)

        self.packages[key] = package
        if package.msg_id in (None, force_msg_id):
            self.__save_state()
            self.__send_messages()

    def _process_message(self, message: str) -> None:
        super()._process_message(message)
        self.__save_state()

    def _post_process(self) -> None:
        super()._post_process()
        self.__send_messages()

    def __save_state(self) -> None:
        self.__prepare_messages()
        StatePersistor().store(LAST_SENT_KEY, self.last_messages)
        StatePersistor().save()

    def __next_message_id(self) -> str | None:
        """
        Returns the next message id to be used
        """
        id = self.current_message_id()
        if id is None:
            return None
        ret = id
        if self.routing_count > 0:
            ret += f";{self.routing_count}"
        self.routing_count += 1
        return ret

    def __prepare_messages(self) -> None:
        self.last_messages = [
            (*key, serialize(package)) for key, package in self.packages.items()
        ]
        self.packages.clear()
        self.routing_count = 0

    def __send_messages(self) -> None:
        for exchange, routing_key, message in self.last_messages:
            self.channel.basic_publish(exchange, routing_key, message.encode())
        self.last_messages = []

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        ...
