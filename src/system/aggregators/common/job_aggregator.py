import logging
from typing import Callable, Generic, TypeVar

from common.messages import End, Message, Start
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
    ends_received: set[str]
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
        self.ends_received = set()
        self.on_finished = on_finished
        self.timer = TimerSender(job_id, comms, aggregator, config)

        self.comms.start_consuming_trips(self.job_id)

    def handle_joined(self, trip: GenericJoinedTrip) -> None:
        self.aggregator.handle_joined(trip)
        self.count += 1
        self.timer.setup_timer()

    def handle_start(self, start: Start) -> None:
        # Only sent to get the job id and start consuming trips,
        # which is done on __init__() so that it only happens once
        pass

    def handle_end(self, end: End) -> None:
        if end.host is None:
            logging.warn("Received End without host id")
            return
        self.ends_received.add(end.host)
        logging.debug(
            f"Job {self.job_id} | Joiner {end.host} finished sending trips"
            f" ({len(self.ends_received)}/{self.config.joiners_count})"
        )
        if len(self.ends_received) < self.config.joiners_count:
            return
        logging.info("Waiting for all trips to be processed")
        self.comms.set_all_trips_done_callback(self.job_id, self.finished)

    def finished(self) -> None:
        logging.info(
            f"Job {self.job_id} | Finished processing all trips. Total processed in"
            f" this node: {self.count}"
        )

        self.timer.remove_timer()
        self.send(End(self.comms.id))
        self.comms.stop_consuming_trips(self.job_id)
        self.on_finished(self)

    def send(self, record: GenericAggregatedRecord | End) -> None:
        self.comms.send(Message(self.job_id, record))
