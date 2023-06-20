from typing import Callable, Generic
import logging

from common.messages.aggregated import (
    GenericAggregatedRecordContr as GenericAggregatedRecord,
)
from common.messages import End, Message

from .job_reducer import JobReducer, Reducer
from .config import Config
from .comms import ReducerComms

ReducerFactory = Callable[[], Reducer[GenericAggregatedRecord]]


class ReductionHandler(Generic[GenericAggregatedRecord]):
    comms: ReducerComms[GenericAggregatedRecord]
    config: Config
    reducer: Reducer[GenericAggregatedRecord]
    jobs: dict[str, JobReducer[GenericAggregatedRecord]]
    job_red_factory: Callable[[str], JobReducer[GenericAggregatedRecord]]

    def __init__(
        self,
        comms: ReducerComms[GenericAggregatedRecord],
        config: Config,
        reducer_factory: ReducerFactory[GenericAggregatedRecord],
    ):
        self.comms = comms
        self.jobs = {}
        self.config = config
        self.job_red_factory = lambda job_id: JobReducer[GenericAggregatedRecord](
            comms, config, reducer_factory(), job_id, self.finished
        )

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def finished(self, job: JobReducer[GenericAggregatedRecord]) -> None:
        logging.info(f"Job {job.job_id} finished")
        self.jobs.pop(job.job_id)

    def handle_aggregated(self, aggregated: GenericAggregatedRecord) -> None:
        self.reducer.handle_aggregated(aggregated)

    def handle_record(self, msg: Message[GenericAggregatedRecord | End]) -> None:
        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            handler = self.job_red_factory(msg.job_id)
            handler.restore_state()
            self.jobs[msg.job_id] = handler

        msg.payload.be_handled_by(self.jobs[msg.job_id])
        self.jobs[msg.job_id].store_state()
