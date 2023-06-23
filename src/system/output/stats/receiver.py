import logging

from common.messages import Message
from common.messages.stats import (
    StatsRecord,
)

from . import StatsStorage
from ..config import Config
from ..comms import SystemCommunication


STATS_STORAGE_KEY = "stats"


class StatsReceiver:
    comms: SystemCommunication
    stats_storage: StatsStorage

    def __init__(self, config: Config, stats: StatsStorage) -> None:
        self.comms = SystemCommunication(config)
        stats.restore_from(STATS_STORAGE_KEY)
        self.stats_storage = stats

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def handle_record(self, msg: Message[StatsRecord]) -> None:
        logging.info(f"Job {msg.job_id} | Received stat {msg.payload.stat_type()}")
        self.stats_storage.store(msg.job_id, msg.payload)
        if self.stats_storage.all_done(msg.job_id):
            logging.info(f"Job {msg.job_id} | Received all stats")

        self.stats_storage.store_to(STATS_STORAGE_KEY)
