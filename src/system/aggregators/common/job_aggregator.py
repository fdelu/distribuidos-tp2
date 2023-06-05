import logging
from typing import Any, Callable, Generic, Protocol

from common.messages import End, Message
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config
from .comms import AggregatorComms


class Aggregator(Protocol[GenericJoinedTrip, GenericAggregatedRecord]):
    def handle_joined(self, trip: GenericJoinedTrip) -> None:
        ...

    def get_value(self) -> GenericAggregatedRecord:
        ...

    def reset(self) -> None:
        ...


class JobAggregator(Generic[GenericJoinedTrip, GenericAggregatedRecord]):
    comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord]
    aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
    config: Config
    job_id: str
    timer: Any | None
    ends_received: int
    count: int = 0
    on_finished: Callable[
        ["JobAggregator[GenericJoinedTrip, GenericAggregatedRecord]"], None
    ]

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
        self.timer = None
        self.on_finished = on_finished

    def handle_joined(self, trip: GenericJoinedTrip) -> None:
        self.aggregator.handle_joined(trip)
        self.count += 1
        if self.timer is None:
            self.setup_timer()

    def handle_end(self) -> None:
        self.ends_received += 1
        logging.debug(
            "A joiner finished sending trips"
            f" ({self.ends_received}/{self.config.joiners_count})"
        )
        if self.ends_received < self.config.joiners_count:
            return
        logging.info("Waiting for all trips to be processed")
        self.comms.set_all_trips_done_callback(self.finished)

    def handle_record(self, record: GenericJoinedTrip | End) -> None:
        record.be_handled_by(self)

    def finished(self) -> None:
        logging.info(
            f"Finished processing all trips. Total processed in this node: {self.count}"
        )
        if self.timer is not None:
            self.comms.cancel_timer(self.timer)
            self.timer = None

        self.send_averages()
        self.send(End())
        self.on_finished(self)

    def timer_callback(self) -> None:
        self.send_averages()
        self.setup_timer()

    def setup_timer(self) -> None:
        self.timer = self.comms.set_timer(
            self.timer_callback, self.config.send_interval_seconds
        )

    def send_averages(self) -> None:
        logging.debug("Sending partial results")
        self.send(self.aggregator.get_value())
        self.aggregator.reset()

    def send(self, record: GenericAggregatedRecord | End) -> None:
        self.comms.send(Message(self.job_id, record))
