import logging
from typing import Callable, Generic

from common.messages import Message
from common.messages.basic import BasicRecord
from common.job_tracker import JobTracker

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
    job_tracker: JobTracker

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
        self.job_tracker = JobTracker()
        self.job_tracker.restore(self.jobs, self.__joiner)

    def finished(self, job_id: str) -> None:
        logging.info(f"Finished job {job_id}")
        self.jobs.pop(job_id)
        self.job_tracker.finished_job(job_id)
        self.comms.finished_job(job_id)

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()

    def cleanup(self) -> None:
        self.comms.close()

    def handle_record(self, msg: Message[BasicRecord]) -> None:
        if msg.job_id in self.job_tracker.state.completed:
            return

        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            self.job_tracker.start_job(msg.job_id)
            handler = self.__joiner(msg.job_id)
            self.jobs[msg.job_id] = handler
        self.jobs[msg.job_id] = msg.payload.be_handled_by(self.jobs[msg.job_id])
        if msg.job_id in self.jobs:
            self.jobs[msg.job_id].store_state()

    def __joiner(self, job_id: str) -> Phase[GenericJoinedTrip]:
        return WeatherStationsPhase[GenericJoinedTrip](
            self.comms,
            self.config,
            self.joiner_factory(),
            job_id,
            self.finished,
        ).restore_state()
