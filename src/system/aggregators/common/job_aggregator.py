import logging
from typing import Callable, Generic, TypeVar

from common.messages import End, Message
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config
from .comms import AggregatorComms
from .aggregator import Aggregator
from .timer import TimerSender

S = TypeVar("S")


class JobAggregator(Generic[GenericJoinedTrip, GenericAggregatedRecord]):
    comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord]
    aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
    config: Config
    job_id: str
    ends_received: int
    count: int = 0
    on_finished: Callable[
        ["JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]"], None
    ]
    timer: TimerSender[GenericJoinedTrip, GenericAggregatedRecord]

    def __init__(
        self,
        comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord],
        config: Config,
        aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord],
        job_id: str,
        on_finished: Callable[
            ["JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]"], None
        ],
    ):
        self.config = config
        self.comms = comms
        self.aggregator = aggregator
        self.job_id = job_id
        self.ends_received = 0
        self.on_finished = on_finished
        self.timer = TimerSender(job_id, comms, aggregator, config)

    def handle_joined(self, trip: GenericJoinedTrip) -> None:
        self.aggregator.handle_joined(trip)
        self.count += 1
        self.timer.setup_timer()

    def handle_start(self) -> None:
        self.comms.start_consuming_trips(self.job_id)

    def handle_end(self) -> None:
        self.ends_received += 1
        logging.debug(
            f"Job {self.job_id} | A joiner finished sending trips"
            f" ({self.ends_received}/{self.config.joiners_count})"
        )
        if self.ends_received < self.config.joiners_count:
            return
        logging.info("Waiting for all trips to be processed")
        self.comms.set_all_trips_done_callback(self.job_id, self.finished)

    def finished(self) -> None:
        logging.info(
            f"Job {self.job_id} | Finished processing all trips. Total processed in"
            f" this node: {self.count}"
        )

        self.timer.remove_timer()
        self.send(End())
        self.comms.stop_consuming_trips(self.job_id)
        self.on_finished(self)

    def send(self, record: GenericAggregatedRecord | End) -> None:
        self.comms.send(Message(self.job_id, record))
