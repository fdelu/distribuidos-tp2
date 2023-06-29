from abc import abstractmethod
from typing import Generic, TypeVar

from shared.serde import serialize, get_generic_types

from common.messages import P
from common.messages.comms import Package
from common.persistence.persistor import StatePersistor
from common.util import register_self_destruct

from ..receive import ReceiveConfig
from ..receive.reliable import ReliableReceive, FilterConfig
from ..base import SystemCommunicationBase
from ..protocol import OUT


PENDING_PACKAGES_KEY = "_pending_packages"
IN = TypeVar("IN", contravariant=True)


class _Unset:
    ...


class ReliableComms(ReliableReceive[P], SystemCommunicationBase, Generic[P, OUT]):
    """
    Comms with send batching capabilities. It will group messages in packages
    with the same routing key and send them when the batch being received is
    done.
    """

    # exchange, routing_key -> batch
    packages: dict[tuple[str, str], Package[OUT]]
    routing_count: int = 0
    add_job_id_to_routing_key: bool

    def __init__(
        self,
        config: ReceiveConfig,
        duplicate_filter_config: FilterConfig | None = None,
        add_job_id_to_routing_key: bool = True,
    ) -> None:
        super().__init__(config, duplicate_filter_config)
        self.packages = {}
        self.add_job_id_to_routing_key = add_job_id_to_routing_key
        self.channel.confirm_delivery()

    def send(
        self, job_id: str, record: OUT, force_msg_id: str | None | _Unset = _Unset()
    ) -> None:
        key = self._get_routing_details(record)
        if self.add_job_id_to_routing_key:
            key = key[0], f"{job_id}.{key[1]}"

        if isinstance(force_msg_id, _Unset):
            package = self.packages.get(key) or Package(
                [], self.__next_message_id(), job_id
            )
            package.messages.append(record)
            self.packages[key] = package
        else:
            package = Package([record], force_msg_id, job_id)
            if key in self.packages:
                raise ValueError(
                    "Can't force message id for the same routing key"
                    " as a buffered package"
                )
            self.packages[key] = package
            self.__save_state()
            self.__send_messages()

    def start_consuming(self) -> None:
        self.__send_pending()
        super().start_consuming()

    def _post_process(self, delivery_tag: int | None) -> None:
        self.routing_count = 0
        self.__save_state()
        super()._post_process(delivery_tag)
        self.__send_messages()

    def __save_state(self) -> None:
        StatePersistor().store(PENDING_PACKAGES_KEY, self.packages)
        StatePersistor().save()

    def __next_message_id(self) -> str | None:
        """
        Returns the next message id to be used
        """
        id = self.current_message_id()
        if id is None:
            return None
        id += f";{self.routing_count}"
        self.routing_count += 1
        return id

    def __send_messages(self, maybe_redelivered: bool = False) -> None:
        if not self.packages:
            return

        register_self_destruct("pre_send")
        for (exchange, routing_key), package in self.packages.items():
            package.maybe_redelivered = maybe_redelivered
            self.channel.basic_publish(
                exchange, routing_key, serialize(package).encode()
            )
        register_self_destruct("post_send")
        self.packages = {}
        self.__save_state()

    def __send_pending(self) -> None:
        out_type = get_generic_types(self, ReliableComms)[1]
        self.packages = (
            StatePersistor().load(PENDING_PACKAGES_KEY, dict[tuple[str, str], Package[out_type]]) or []  # type: ignore # noqa
        ) or {}
        self.__send_messages(maybe_redelivered=True)

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        ...
