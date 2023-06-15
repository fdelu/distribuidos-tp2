from typing import Any, Callable, Generic
from time import time
from shutil import move
import logging
import os

from shared.serde import deserialize
from common.messages import Package

from . import CommsReceive, IN, ReceiveConfig

MINUTES_TO_KEEP = 3

LOG_FILE = "/received.log"
LOG_FILE_TMP = "/tmp/received.log.tmp"


class ReliableReceive(Generic[IN], CommsReceive[IN]):
    current_msg_id: str | None = None
    latest_messages: dict[int, list[str]] = {}  # minuto -> lista de ids
    batch_done_callback: Callable[[], None] | None = None

    def __init__(self, config: ReceiveConfig, with_interrupt: bool = True) -> None:
        self.__load_msg_ids()
        super().__init__(config, with_interrupt)

    def current_message_id(self) -> str | None:
        return self.current_msg_id

    def set_batch_done_callback(self, callback: Callable[[], None]) -> None:
        self.batch_done_callback = callback

    def _process_message(self, message: str) -> None:
        batch = self.__deserialize_batch(message)
        if not self.__pre_process(batch):
            return

        if self.callback is not None:
            for msg in batch.messages:
                self.callback(msg)
        if self.batch_done_callback:
            self.batch_done_callback()

        self.__post_process()

    def __deserialize_batch(self, message: str) -> Package[IN]:
        return deserialize(Package[self.in_type], message)  # type: ignore

    def __pre_process(
        self,
        package: Package[Any],
    ) -> bool:
        if package.msg_id and self.__already_received(package.msg_id):
            logging.warn(
                f"Already received {package.msg_id}\n{str(package.messages)[:100]}"
            )
            return False
        self.current_msg_id = package.msg_id
        if package.msg_id:
            self.__add_msg_id(package.msg_id)
        return True

    def __post_process(self) -> None:
        if not os.path.exists(LOG_FILE_TMP):
            # must have received batches without msg_id
            return
        move(LOG_FILE_TMP, LOG_FILE)

    def __already_received(self, msg_id: str) -> bool:
        return any(msg_id in ids for ids in self.latest_messages.values())

    def __add_msg_id(self, *msg_ids: str) -> None:
        minute = int(time() // 60)
        self.latest_messages.setdefault(minute, []).extend(msg_ids)

        minute_to_remove = minute - MINUTES_TO_KEEP
        while minute_to_remove in self.latest_messages:
            self.latest_messages.pop(minute_to_remove)
            minute_to_remove -= 1

        with open(LOG_FILE_TMP, "w") as f:
            for ids in self.latest_messages.values():
                f.write("\n".join(ids) + "\n")

    def __load_msg_ids(self) -> None:
        if not os.path.exists(LOG_FILE):
            logging.debug("No message ids log file found")
            return

        logging.debug("Loading previous message ids")
        with open(LOG_FILE, "r") as f:
            self.__add_msg_id(*f.read().splitlines())