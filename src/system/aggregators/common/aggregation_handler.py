import logging
from typing import Callable, Generic

from common.messages import End, Message, Start
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config
from .comms import AggregatorComms
from .job_aggregator import JobAggregator
from .aggregator import Aggregator


class AggregationHandler(Generic[GenericJoinedTrip, GenericAggregatedRecord]):
    comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord]
    aggregator_factory: Callable[
        [], Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
    ]
    jobs: dict[str, JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]]
    config: Config

    def __init__(
        self,
        comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord],
        aggregator_factory: Callable[
            [], Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
        ],
        config: Config,
    ):
        self.config = config
        self.comms = comms
        self.jobs = {}
        self.aggregator_factory = aggregator_factory

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def finished(self, job_id: str) -> None:
        logging.info(f"Finished job {job_id}")
        self.jobs.pop(job_id)

    def handle_record(self, msg: Message[GenericJoinedTrip | End | Start]) -> None:
        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            handler = self._job_handler(msg.job_id)
            handler.restore_state()
            self.jobs[msg.job_id] = handler

        msg.payload.be_handled_by(self.jobs[msg.job_id])
        self.jobs[msg.job_id].store_state()

    def _job_handler(
        self, job_id: str
    ) -> JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]:
        return JobAggregator[GenericJoinedTrip, GenericAggregatedRecord](
            self.comms, self.config, self.aggregator_factory(), job_id, self.finished
        )
