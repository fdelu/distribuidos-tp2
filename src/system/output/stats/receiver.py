import logging
from typing import Protocol

from common.messages.stats import (
    RainAverages,
    StatsRecord,
    StatType,
    YearCounts,
    CityAverages,
)

from . import Stats
from ..config import Config
from ..comms import SystemCommunication


class StatListener(Protocol):
    def received(self, type: StatType) -> None:
        """
        This method is called when a new stat is received
        """
        ...


class StatsReceiver:
    comms: SystemCommunication
    stats: Stats
    listeners: list[StatListener] = []

    def __init__(self, config: Config, stats: Stats) -> None:
        self.comms = SystemCommunication(config)
        self.stats = stats

    def add_listener(self, listener: StatListener) -> None:
        self.listeners.append(listener)

    def __notify_listeners(self, type: StatType) -> None:
        for listener in self.listeners:
            listener.received(type)

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def handle_rain_averages(self, stat: RainAverages) -> None:
        logging.info("Received rain averages")
        with self.stats.lock:
            self.stats.rain_averages = stat
        self.__notify_listeners(StatType.RAIN)

    def handle_year_counts(self, stat: YearCounts) -> None:
        logging.info("Received year counts")
        with self.stats.lock:
            self.stats.year_counts = stat
        self.__notify_listeners(StatType.YEAR)

    def handle_city_averages(self, stat: CityAverages) -> None:
        logging.info("Received city averages")
        with self.stats.lock:
            self.stats.city_averages = stat
        self.__notify_listeners(StatType.CITY)

    def handle_record(self, record: StatsRecord) -> None:
        record.be_handled_by(self)
        if self.stats.all_done():
            logging.info("Received all stats")
