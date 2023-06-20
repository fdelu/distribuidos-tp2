from typing import Any, Generic
import logging

from shared.serde import deserialize
from common.messages import Package
from common.persistence.persistor import StatePersistor

from . import CommsReceive, IN, ReceiveConfig

RECEIVED_MESSAGES_KEY = "_received_messages"


class ReliableReceive(Generic[IN], CommsReceive[IN]):
    current_msg_id: str | None = None
    received_messages: set[str]

    def __init__(self, config: ReceiveConfig, with_interrupt: bool = True) -> None:
        self.received_messages = (
            StatePersistor().load(RECEIVED_MESSAGES_KEY, set[str]) or set()
        )
        super().__init__(config, with_interrupt)

    def current_message_id(self) -> str | None:
        return self.current_msg_id

    def _process_message(self, message: str) -> None:
        batch = self.__deserialize_batch(message)
        if not self.__pre_process(batch):
            return

        if self.callback is not None:
            for msg in batch.messages:
                self.callback(msg)

    def _post_process(self) -> None:
        self.current_msg_id = None

    def __deserialize_batch(self, message: str) -> Package[IN]:
        return deserialize(Package[self.in_type], message)  # type: ignore

    def __pre_process(
        self,
        package: Package[Any],
    ) -> bool:
        if package.msg_id is None:
            self.current_msg_id = None
            return True

        if package.msg_id in self.received_messages:
            logging.warn(
                f"Already received {package.msg_id}\n{str(package.messages)[:100]}"
            )
            return False

        self.current_msg_id = package.msg_id
        self.received_messages.add(package.msg_id)
        StatePersistor().store(RECEIVED_MESSAGES_KEY, self.received_messages)
        return True
