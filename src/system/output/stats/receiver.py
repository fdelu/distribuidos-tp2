import logging
from typing import Protocol

from common.messages import Message
from common.messages.stats import (
    StatsRecord,
    StatType,
)

from . import StatsStorage
from ..config import Config
from ..comms import SystemCommunication


class StatListener(Protocol):
    def received(self, job_id: str, type: StatType) -> None:
        """
        This method is called when a new stat is received
        """
        ...


class StatsReceiver:
    comms: SystemCommunication
    stats_storage: StatsStorage
    listeners: list[StatListener]

    def __init__(self, config: Config, stats: StatsStorage) -> None:
        self.comms = SystemCommunication(config)
        self.stats_storage = stats
        self.listeners = []

    def add_listener(self, listener: StatListener) -> None:
        self.listeners.append(listener)

    def __notify_listeners(self, job_id: str, type: StatType) -> None:
        for listener in self.listeners:
            listener.received(job_id, type)

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def handle_record(self, msg: Message[StatsRecord]) -> None:
        logging.info(f"Job {msg.job_id} | Received stat {msg.payload.stat_type()}")
        self.stats_storage.store(msg.job_id, msg.payload)
        self.__notify_listeners(msg.job_id, msg.payload.stat_type())
        if self.stats_storage.all_done(msg.job_id):
            logging.info(f"Job {msg.job_id} | Received all stats")
