from typing import Generic, Any
from functools import cached_property

from shared.serde import get_generic_types
from common.messages import Message, P
from common.messages.comms import Package
from common.util import register_self_destruct

from .. import CommsReceive, ReceiveConfig
from .duplicate_filters.simple import DuplicateFilter, DuplicateFilterSimple
from .duplicate_filters.distributed import DuplicateFilterDistributed, FilterConfig

__all__ = ["FilterConfig"]


class ReliableReceive(Generic[P], CommsReceive[Message[P]]):
    current_msg_id: str | None = None
    duplicate_filter: DuplicateFilter[P]

    def __init__(
        self,
        config: ReceiveConfig,
        distributed_filter_config: FilterConfig | None = None,
        with_interrupt: bool = True,
    ) -> None:
        super().__init__(config, with_interrupt)
        if distributed_filter_config:
            self.duplicate_filter = DuplicateFilterDistributed(
                self, distributed_filter_config
            )
            self.duplicate_filter.load_definitions()
        else:
            self.duplicate_filter = DuplicateFilterSimple(self)

    @cached_property
    def in_type(self) -> Any:
        """
        Input type (resolved P TypeVar from ReliableReceive[IN])
        """
        return get_generic_types(self, ReliableReceive)[0]

    def current_message_id(self) -> str | None:
        return self.current_msg_id

    def handle_package(self, package: Package[P], delivery_tag: int | None) -> None:
        self.current_msg_id = package.msg_id

        if self.callback is not None:
            for msg in package.messages:
                self.callback(Message(package.job_id, msg))

        self._post_process(delivery_tag)

    def finished_job(self, job_id: str) -> None:
        self.duplicate_filter.clear_job(job_id)

    def _process_message(
        self, data: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        self.duplicate_filter.received_message(data, queue, delivery_tag, redelivered)

    def _post_process(self, delivery_tag: int | None) -> None:
        """
        Acknowledges the message and restarts the current_msg_id
        """
        if delivery_tag is not None:
            register_self_destruct("pre_ack")
            self.channel.basic_ack(delivery_tag)
        self.current_msg_id = None

    def _messages_left(self, queue: str) -> int | None:
        """
        Returns the number of messages left in the given queue
        """
        c = self.channel.queue_declare(queue).method.message_count
        if c is None:
            return None
        return c + self.duplicate_filter.pending_count(queue)
