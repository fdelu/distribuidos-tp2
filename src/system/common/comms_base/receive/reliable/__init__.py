from typing import Generic

from common.messages.comms import Package

from .. import CommsReceive, IN, ReceiveConfig
from .duplicate.simple import DuplicateFilter, DuplicateFilterSimple
from .duplicate.distributed import DuplicateFilterDistributed, FilterConfig

__all__ = ["FilterConfig"]


class ReliableReceive(Generic[IN], CommsReceive[IN]):
    current_msg_id: str | None = None
    duplicate_filter: DuplicateFilter[IN]

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

    def current_message_id(self) -> str | None:
        return self.current_msg_id

    def handle_package(self, package: Package[IN], delivery_tag: int | None) -> None:
        self.current_msg_id = package.msg_id

        if self.callback is not None:
            for msg in package.messages:
                self.callback(msg)

        self._post_process(delivery_tag)

    def _process_message(
        self, data: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        self.duplicate_filter.received_message(data, queue, delivery_tag, redelivered)

    def _post_process(self, delivery_tag: int | None) -> None:
        """
        Acknowledges the message and restarts the current_msg_id
        """
        if delivery_tag is not None:
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
