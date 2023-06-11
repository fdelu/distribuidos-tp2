import logging

from common.messages import Message
from common.messages.raw import RawRecord

from .config import Config
from .comms import SystemCommunication
from .phases import Phase
from .phases.weather_stations import WeatherStationsPhase


class RecordParser:
    comms: SystemCommunication
    jobs: dict[str, Phase]

    def __init__(self, config: Config) -> None:
        self.comms = SystemCommunication(config)
        self.jobs = {}

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def finished(self, job: Phase) -> None:
        logging.info(f"Finished job {job.job_id}")
        self.jobs.pop(job.job_id)

    def handle_record(self, msg: Message[RawRecord]) -> None:
        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            self.jobs[msg.job_id] = WeatherStationsPhase(
                self.comms, msg.job_id, self.finished
            )
        self.jobs[msg.job_id] = self.jobs[msg.job_id].handle_record(msg.payload)
