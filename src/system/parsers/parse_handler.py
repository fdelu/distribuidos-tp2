import logging

from common.messages import Message
from common.messages.raw import RawRecord
from common.job_tracker import JobTracker

from .comms import SystemCommunication
from .job_parser import JobParser


class ParseHandler:
    comms: SystemCommunication
    jobs: dict[str, JobParser]
    job_tracker: JobTracker

    def __init__(self, comms: SystemCommunication) -> None:
        self.comms = comms
        self.jobs = {}
        self.job_tracker = JobTracker()
        self.job_tracker.restore(self.jobs, self.__parser)

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def finished(self, job: JobParser) -> None:
        logging.info(f"Finished job {job.job_id}")
        self.jobs.pop(job.job_id)
        self.job_tracker.finished_job(job.job_id)

    def handle_record(self, msg: Message[RawRecord]) -> None:
        if msg.job_id in self.job_tracker.state.completed:
            return

        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            self.job_tracker.start_job(msg.job_id)
            parser = self.__parser(msg.job_id)
            self.jobs[msg.job_id] = parser

        msg.payload.be_handled_by(self.jobs[msg.job_id])
        if msg.job_id in self.jobs:
            self.jobs[msg.job_id].store_state()

    def __parser(self, job_id: str) -> JobParser:
        return JobParser(self.comms, job_id, self.finished)
