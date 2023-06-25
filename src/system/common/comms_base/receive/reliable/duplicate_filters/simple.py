import logging
from typing import Generic

from shared.serde import deserialize
from common.messages.comms import Package

from . import DuplicateFilter, IN

__all__ = ["DuplicateFilter"]


class DuplicateFilterSimple(DuplicateFilter[IN], Generic[IN]):
    def received_message(
        self, message: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        package = self.__deserialize_package(message)
        if package.msg_id and (redelivered or package.maybe_redelivered):
            if self._was_processed(package.job_id, package.msg_id):
                logging.warn(
                    f"Already received {package.msg_id} -"
                    f" {str(package.messages)[:100]}..."
                )
                self._ack(delivery_tag)
                return

            self._processed(package.job_id, package.msg_id)
        self.comms.handle_package(package, delivery_tag)

    def __deserialize_package(self, message: str) -> Package[IN]:
        return deserialize(Package[self.comms.in_type], message)  # type: ignore

    def pending_count(self, queue: str) -> int:
        return 0
