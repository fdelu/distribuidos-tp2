import logging
from typing import Callable, Generic

from common.messages import Message
from common.messages.basic import BasicRecord


from .phases import Phase, Joiner
from .phases.weather_stations import WeatherStationsPhase
from .comms import JoinerComms, GenericJoinedTrip
from .config import Config

JoinerFactory = Callable[[], Joiner[GenericJoinedTrip]]


class JoinHandler(Generic[GenericJoinedTrip]):
    comms: JoinerComms[GenericJoinedTrip]
    config: Config
    jobs: dict[str, Phase[GenericJoinedTrip]]
    joiner_factory: JoinerFactory[GenericJoinedTrip]

    def __init__(
        self,
        config: Config,
        comms: JoinerComms[GenericJoinedTrip],
        joiner_factory: JoinerFactory[GenericJoinedTrip],
    ) -> None:
        self.comms = comms
        self.config = config
        self.jobs = {}
        self.joiner_factory = joiner_factory

    def finished(self, job: Phase[GenericJoinedTrip]) -> None:
        logging.info(f"Finished job {job.job_id}")
        self.jobs.pop(job.job_id)

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def handle_record(self, msg: Message[BasicRecord]) -> None:
        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            self.jobs[msg.job_id] = WeatherStationsPhase(
                self.comms,
                self.config,
                self.joiner_factory(),
                msg.job_id,
                self.finished,
            )
        self.jobs[msg.job_id] = msg.payload.be_handled_by(self.jobs[msg.job_id])
