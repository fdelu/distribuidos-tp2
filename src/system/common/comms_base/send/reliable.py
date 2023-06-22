from abc import abstractmethod
from typing import Generic, TypeVar

from shared.serde import serialize, get_generic_types

from common.messages.comms import Package
from common.persistence.persistor import StatePersistor

from ..receive import ReceiveConfig
from ..receive.reliable import ReliableReceive, FilterConfig
from ..base import SystemCommunicationBase
from ..protocol import OUT


PENDING_MESSAGES_KEY = "_pending_messages"
IN = TypeVar("IN", contravariant=True)


class _Unset:
    ...


class ReliableComms(ReliableReceive[IN], SystemCommunicationBase, Generic[IN, OUT]):
    """
    Comms with send batching capabilities. It will group messages in packages
    with the same routing key and send them when the batch being received is
    done.
    """

    # exchange, routing_key -> batch
    pending_packages: dict[tuple[str, str], Package[OUT]]
    packages: dict[tuple[str, str], Package[OUT]]
    routing_count: int = 0

    def __init__(
        self,
        config: ReceiveConfig,
        duplicate_filter_config: FilterConfig | None = None,
    ) -> None:
        super().__init__(config, duplicate_filter_config)
        self.packages = {}
        self.pending_packages = {}

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

    def start_consuming(self) -> None:
        self.__send_pending()
        super().start_consuming()

    def _post_process(self, delivery_tag: int | None) -> None:
        self.__save_state()
        super()._post_process(delivery_tag)
        self.__send_messages()

    def __save_state(self) -> None:
        self.__prepare_send()
        StatePersistor().store(PENDING_MESSAGES_KEY, self.pending_packages)
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

    def __prepare_send(self) -> None:
        self.pending_packages = self.packages
        self.packages = {}
        self.routing_count = 0

    def __send_messages(self, maybe_redelivered: bool = False) -> None:
        for (exchange, routing_key), package in self.pending_packages.items():
            package.maybe_redelivered = maybe_redelivered
            self.channel.basic_publish(
                exchange, routing_key, serialize(package).encode()
            )
        self.pending_packages = {}

    def __send_pending(self) -> None:
        out_type = get_generic_types(self, ReliableComms)[1]
        pending: dict[tuple[str, str], Package[OUT]] | None = (
            StatePersistor().load(PENDING_MESSAGES_KEY, dict[tuple[str, str], out_type]) or []  # type: ignore # noqa
        )
        if pending:
            self.pending_packages = pending
            self.__send_messages(maybe_redelivered=True)

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        ...
