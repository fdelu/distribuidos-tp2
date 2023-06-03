from typing import Any
from time import time
from shutil import move
import logging
import os

from common.messages import Batch


MINUTES_TO_KEEP = 3

LOG_FILE = "/received.log"
LOG_FILE_TMP = "/tmp/received.log.tmp"


class RealiableReceive:
    current_msg_id: str | None = None
    latest_messages: dict[int, list[str]] = {}  # minuto -> lista de ids

    def __init__(self) -> None:
        self.__load_msg_ids()

    def pre_process(
        self,
        batch: Batch[Any],
    ) -> bool:
        if batch.msg_id and self.__already_received(batch.msg_id):
            logging.info(f"Already received {batch.msg_id}")
            return False
        self.current_msg_id = batch.msg_id
        if batch.msg_id:
            self.__add_msg_id(batch.msg_id)
        return True

    def post_process(self) -> None:
        if not os.path.exists(LOG_FILE_TMP):
            # must have received batches without msg_id
            return
        move(LOG_FILE_TMP, LOG_FILE)

    def current_message_id(self) -> str | None:
        return self.current_msg_id

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
