import logging

from common.messages import Message
from common.messages.raw import RawRecord

from .comms import SystemCommunication
from .job_parser import JobParser


class ParseHandler:
    comms: SystemCommunication
    jobs: dict[str, JobParser]

    def __init__(self, comms: SystemCommunication) -> None:
        self.comms = comms
        self.jobs = {}

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def finished(self, job: JobParser) -> None:
        logging.info(f"Finished job {job.job_id}")
        self.jobs.pop(job.job_id)

    def handle_record(self, msg: Message[RawRecord]) -> None:
        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            self.jobs[msg.job_id] = JobParser(self.comms, msg.job_id, self.finished)
        msg.payload.be_handled_by(self.jobs[msg.job_id])
