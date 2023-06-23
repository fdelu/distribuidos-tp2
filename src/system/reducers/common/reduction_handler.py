from typing import Callable, Generic
import logging

from common.messages.aggregated import (
    GenericAggregatedRecordContr as GenericAggregatedRecord,
)
from common.messages import End, Message
from common.job_tracker import JobTracker

from .job_reducer import JobReducer, Reducer
from .config import Config
from .comms import ReducerComms


class ReductionHandler(Generic[GenericAggregatedRecord]):
    comms: ReducerComms[GenericAggregatedRecord]
    config: Config
    jobs: dict[str, JobReducer[GenericAggregatedRecord]]
    reducer_factory: Callable[[], Reducer[GenericAggregatedRecord]]

    job_tracker: JobTracker

    def __init__(
        self,
        comms: ReducerComms[GenericAggregatedRecord],
        config: Config,
        reducer_factory: Callable[[], Reducer[GenericAggregatedRecord]],
    ):
        self.comms = comms
        self.jobs = {}
        self.config = config
        self.reducer_factory = reducer_factory
        self.job_tracker = JobTracker()
        self.job_tracker.restore(self.jobs, self.__reducer)

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def finished(self, job: JobReducer[GenericAggregatedRecord]) -> None:
        logging.info(f"Job {job.job_id} finished")
        self.jobs.pop(job.job_id)
        self.job_tracker.finished_job(job.job_id)

    def handle_record(self, msg: Message[GenericAggregatedRecord | End]) -> None:
        if msg.job_id in self.job_tracker.state.completed:
            return

        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            self.job_tracker.start_job(msg.job_id)
            handler = self.__reducer(msg.job_id)
            self.jobs[msg.job_id] = handler

        msg.payload.be_handled_by(self.jobs[msg.job_id])
        if msg.job_id in self.jobs:
            self.jobs[msg.job_id].store_state()

    def __reducer(self, job_id: str) -> JobReducer[GenericAggregatedRecord]:
        reducer = JobReducer[GenericAggregatedRecord](
            self.comms, self.config, self.reducer_factory(), job_id, self.finished
        )
        reducer.restore_state()
        return reducer
