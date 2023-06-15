import logging
from typing import Callable, Generic

from common.messages import End, Message, Start
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config
from .comms import AggregatorComms
from .job_aggregator import JobAggregator
from .aggregator import Aggregator

AggregatorFactory = Callable[[], Aggregator[GenericJoinedTrip, GenericAggregatedRecord]]


class AggregationHandler(Generic[GenericJoinedTrip, GenericAggregatedRecord]):
    comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord]
    job_agg_factory: Callable[
        [str], JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]
    ]
    jobs: dict[str, JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]]
    config: Config

    def __init__(
        self,
        comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord],
        aggregator_factory: AggregatorFactory[
            GenericJoinedTrip, GenericAggregatedRecord
        ],
        config: Config,
    ):
        self.config = config
        self.comms = comms
        self.jobs = {}
        self.job_agg_factory = lambda job_id: JobAggregator(
            self.comms, self.config, aggregator_factory(), job_id, self.finished
        )

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

    def finished(
        self, job: JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]
    ) -> None:
        logging.info(f"Finished job {job.job_id}")
        self.jobs.pop(job.job_id)

    def handle_record(self, msg: Message[GenericJoinedTrip | End | Start]) -> None:
        if msg.job_id not in self.jobs:
            logging.info(f"Starting job {msg.job_id}")
            self.jobs[msg.job_id] = self.job_agg_factory(msg.job_id)
        msg.payload.be_handled_by(self.jobs[msg.job_id])
