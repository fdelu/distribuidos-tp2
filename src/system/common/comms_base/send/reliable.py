from abc import abstractmethod
from typing import Generic, TypeVar

from shared.serde.internal.serialize import serialize

from ...messages import Package

from ..protocol import CommsProtocol, ReceiveReliableProtocol, OUT, ConfigProtocol


OUT_INNER = TypeVar("OUT_INNER")
MAX_DELAY_S = 3.0


class _Unset:
    ...


class ReliableSend(CommsProtocol, ReceiveReliableProtocol, Generic[OUT]):
    """
    Comms with send batching capabilities. It will group messages in packages
    with the same routing key and send them when the batch being received is
    done.
    """

    packages: dict[tuple[str, str], "Package[OUT]"]  # exchange, routing_key -> batch
    routing_count: int = 0

    def __init__(self, config: ConfigProtocol) -> None:
        super().__init__(config)
        self.packages = {}
        self.set_batch_done_callback(self.__flush)

    def send(self, record: OUT, force_msg_id: str | None | _Unset = _Unset()) -> None:
        key = self._get_routing_details(record)
        if not isinstance(force_msg_id, _Unset):
            package = Package([record], force_msg_id)
            self.__send_package(*key, package)
            return

        package = self.packages.get(key) or Package([], self.__next_message_id())
        package.messages.append(record)
        if package.msg_id is None:
            self.__send_package(*key, package)
            return
        self.packages[key] = package

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

    def __flush(self) -> None:
        """
        Sends all packages that are currently in the queue
        """
        for key, package in self.packages.items():
            self.__send_package(*key, package)
        self.packages.clear()
        self.routing_count = 0

    def __send_package(
        self, exchange: str, routing_key: str, batch: Package[OUT]
    ) -> None:
        """
        Sends the given batch to the given exchange and routing key
        """
        msg = serialize(batch)
        self.channel.basic_publish(exchange, routing_key, msg.encode())

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        ...
