import logging
from dataclasses import dataclass
from typing import Callable, Generic

from common.messages import End, Start
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord
from common.persistence import WithState, StatePersistor

from .config import Config
from .comms import AggregatorComms
from .aggregator import Aggregator
from .timer import TimerSender


@dataclass
class State:
    ends_received: set[str]
    count: int


class JobAggregator(
    Generic[GenericJoinedTrip, GenericAggregatedRecord], WithState[State]
):
    comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord]
    aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
    config: Config
    job_id: str
    on_finished: Callable[[str], None]
    timer: TimerSender[GenericJoinedTrip, GenericAggregatedRecord]

    def __init__(
        self,
        comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord],
        config: Config,
        aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord],
        job_id: str,
        on_finished: Callable[[str], None],
    ):
        super().__init__(State(set(), 0))
        self.config = config
        self.comms = comms
        self.aggregator = aggregator
        self.job_id = job_id
        self.on_finished = on_finished
        self.timer = TimerSender(
            job_id, comms, aggregator, config, self._aggregator_store_key()
        )

        self.comms.start_consuming_trips(self.job_id)

    def handle_joined(self, trip: GenericJoinedTrip) -> None:
        self.aggregator.handle_joined(trip)
        self.state.count += 1
        self.timer.setup_timer()

    def handle_start(self, start: Start) -> None:
        # Only sent to get the job id and start consuming trips,
        # which is done on __init__() so that it only happens once
        pass

    def handle_end(self, end: End) -> None:
        if end.host is None:
            logging.warn("Received End without host id")
            return
        self.state.ends_received.add(end.host)
        logging.debug(
            f"Job {self.job_id} | Joiner {end.host} finished sending trips"
            f" ({len(self.state.ends_received)}/{self.config.joiners_count})"
        )
        self.check_ends()

    def check_ends(self) -> None:
        if len(self.state.ends_received) < self.config.joiners_count:
            return
        logging.info(f"Job {self.job_id} | Waiting for all trips to be processed")
        self.comms.set_all_trips_done_callback(self.job_id, self.finished)

    def finished(self) -> None:
        logging.info(
            f"Job {self.job_id} | Finished processing all trips. Total processed in"
            f" this node: {self.state.count}"
        )

        self.timer.remove_timer()
        self.comms.stop_consuming_trips(self.job_id)
        StatePersistor().remove(self._aggregator_store_key())
        StatePersistor().remove(self._control_store_key())
        self.on_finished(self.job_id)
        self.send(End(self.comms.id))

    def send(self, record: GenericAggregatedRecord | End) -> None:
        self.comms.send(self.job_id, record)

    def restore_state(self) -> None:
        self.restore_from(self._control_store_key())
        self.aggregator.restore_from(self._aggregator_store_key())
        self.check_ends()

    def store_state(self) -> None:
        self.store_to(self._control_store_key())
        self.aggregator.store_to(self._aggregator_store_key())

    def _control_store_key(self) -> str:
        return f"control_{self.job_id}"

    def _aggregator_store_key(self) -> str:
        return f"aggregator_{self.job_id}"
