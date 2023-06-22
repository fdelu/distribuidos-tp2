from typing import Generic

from common.messages.comms import Package

from . import CommsReceive, IN, ReceiveConfig
from .duplicate.simple import DuplicateFilter, DuplicateFilterSimple


class ReliableReceive(Generic[IN], CommsReceive[IN]):
    current_msg_id: str | None = None
    duplicate_filter: DuplicateFilter[IN] | None

    def __init__(
        self,
        config: ReceiveConfig,
        duplicate_filter: DuplicateFilter[IN] | None = None,
        with_interrupt: bool = True,
    ) -> None:
        super().__init__(config, with_interrupt)
        self.duplicate_filter = duplicate_filter

    def current_message_id(self) -> str | None:
        return self.current_msg_id

    def handle_package(self, package: Package[IN], delivery_tag: int | None) -> None:
        self.current_msg_id = package.msg_id

        if self.callback is not None:
            for msg in package.messages:
                self.callback(msg)

        self._post_process(delivery_tag)

    def _process_message(
        self, data: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        if self.duplicate_filter is None:
            self.duplicate_filter = DuplicateFilterSimple[self.in_type](self)  # type: ignore # noqa
        self.duplicate_filter.received_message(data, delivery_tag, redelivered)

    def _post_process(self, delivery_tag: int | None) -> None:
        """
        Acknowledges the message and restarts the current_msg_id
        """
        if delivery_tag is not None:
            self.channel.basic_ack(delivery_tag)
        self.current_msg_id = None
