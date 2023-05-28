import logging
from typing import Any, Generic, Protocol

from common.messages import End
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


class AggregationHandler(Generic[GenericJoinedTrip, GenericAggregatedRecord]):
    comms: AggregatorComms[GenericJoinedTrip | End, GenericAggregatedRecord | End]
    aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
    config: Config
    timer: Any | None
    ends_received: int
    count: int = 0

    def __init__(
        self,
        comms: AggregatorComms[GenericJoinedTrip | End, GenericAggregatedRecord | End],
        aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord],
        config: Config,
    ):
        self.config = config
        self.comms = comms
        self.aggregator = aggregator
        self.ends_received = 0
        self.timer = None

    def run(self) -> None:
        self.comms.set_callback(self.handle_record)
        self.comms.start_consuming()
        self.comms.close()

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
        self.comms.send(End())
        self.comms.stop_consuming()

    def timer_callback(self) -> None:
        self.send_averages()
        self.setup_timer()

    def setup_timer(self) -> None:
        self.timer = self.comms.set_timer(
            self.timer_callback, self.config.send_interval_seconds
        )

    def send_averages(self) -> None:
        logging.debug("Sending partial results")
        self.comms.send(self.aggregator.get_value())
        self.aggregator.reset()
