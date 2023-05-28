import logging

from common.messages.raw import RawRecord

from .config import Config
from .comms import SystemCommunication
from .phases import Phase
from .phases.weather_stations import WeatherStationsPhase


class RecordParser:
    comms: SystemCommunication
    phase: Phase

    def __init__(self, config: Config):
        self.comms = SystemCommunication(config)
        self.phase = WeatherStationsPhase(self.comms)

    def run(self):
        logging.info("Receiving weather & stations")
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def handle_record(self, raw_record: RawRecord):
        self.phase = self.phase.handle_record(raw_record)
