import logging
from typing import Generic, TypeVar, Any, Protocol
from abc import abstractmethod, ABC
from datetime import datetime

from common.comms_base.protocol import CommsProtocol
from common.messages.comms import PackageHandler
from common.persistence import StatePersistor
from common.util import register_self_destruct


RECEIVED_PACKAGES_KEY = "_received_packages"
KEEP_MINUTES = 2

IN = TypeVar("IN", covariant=True)
T = TypeVar("T", contravariant=True)


class PackageComms(PackageHandler[T], CommsProtocol, Protocol[T]):
    @property
    def in_type(self) -> Any:
        pass

    def _start_consuming_from(self, queue: str) -> None:
        pass


class DuplicateFilter(Generic[IN], ABC):
    # job_id -> timestamp -> msg_id
    received_messages: dict[str, dict[datetime, set[str]]]
    comms: PackageComms[IN]

    def __init__(self, package_handler: PackageComms[IN]) -> None:
        self.received_messages = {}
        self.comms = package_handler

    def _ack(self, delivery_tag: int | None) -> None:
        if delivery_tag is not None:
            register_self_destruct("pre_ack")
            self.comms.channel.basic_ack(delivery_tag)

    def _nack(self, delivery_tag: int | None) -> None:
        if delivery_tag is not None:
            self.comms.channel.basic_reject(delivery_tag, requeue=True)

    def _processed(self, job_id: str, msg_id: str) -> None:
        """
        Mark the message as processed.
        """
        last_ids = self.received_messages.setdefault(job_id, {})
        timestamp = datetime.now().replace(second=0, microsecond=0)
        last_ids.setdefault(timestamp, set())
        last_ids[timestamp].add(msg_id)
        self.__remove_old(last_ids, timestamp)
        StatePersistor().append(self.__key(job_id), msg_id)

    def _was_processed(self, job_id: str, msg_id: str) -> bool:
        """
        Check if the message was already processed. Checks first
        if it's available in memory, then in disk.
        """
        last_ids = self.received_messages.get(job_id, {})
        for ids_per_min in last_ids.values():
            if msg_id in ids_per_min:
                return True
        logging.debug(f"Iterating over all past message ids for job {job_id} in disk")
        return msg_id in StatePersistor().iter(self.__key(job_id))

    def clear_job(self, job_id: str) -> None:
        """
        Clears all the messages for the given job_id
        """
        self.received_messages.pop(job_id, None)
        # StatePersistor().remove(self.__key(job_id))

    def __remove_old(self, ids: dict[datetime, set[str]], current: datetime) -> None:
        for timestamp in list(ids):
            if (current - timestamp).total_seconds() / 60 <= KEEP_MINUTES:
                break
            ids.pop(timestamp, None)

    def __key(self, job_id: str) -> str:
        return f"{RECEIVED_PACKAGES_KEY}_{job_id}"

    @abstractmethod
    def received_message(
        self, message: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        ...

    @abstractmethod
    def pending_count(self, queue: str) -> int:
        """
        Returns the amount of pending checks for the given queue.
        """
        ...
