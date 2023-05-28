import logging
from typing import Generic

from common.messages.basic import BasicRecord


from .phases import Phase, Joiner
from .phases.weather_stations import WeatherStationsPhase
from .comms import JoinerComms, GenericJoinedTrip
from .config import Config


class JoinHandler(Generic[GenericJoinedTrip]):
    comms: JoinerComms[GenericJoinedTrip]
    config: Config
    joiner: Joiner
    phase: Phase

    def __init__(self, config: Config, comms: JoinerComms, joiner: Joiner):
        self.comms = comms
        self.config = config
        self.phase = WeatherStationsPhase(self.comms, config, joiner)

    def run(self):
        logging.info("Receiving weather & stations")
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def handle_record(self, record: BasicRecord):
        self.phase = self.phase.handle_record(record)
