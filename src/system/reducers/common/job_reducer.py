from dataclasses import dataclass
from typing import Callable, Generic, Protocol
import logging

from common.messages.aggregated import (
    GenericAggregatedRecordContr as GenericAggregatedRecord,
)
from common.messages.stats import StatsRecord
from common.messages import End
from common.persistence import WithState, WithStateProtocol, StatePersistor

from .config import Config
from .comms import ReducerComms


class Reducer(WithStateProtocol, Protocol[GenericAggregatedRecord]):
    def handle_aggregated(self, aggregated: GenericAggregatedRecord) -> None:
        ...

    def get_value(self) -> StatsRecord:
        ...


@dataclass
class State:
    ends_received: set[str]


class JobReducer(Generic[GenericAggregatedRecord], WithState[State]):
    comms: ReducerComms[GenericAggregatedRecord]
    config: Config
    reducer: Reducer[GenericAggregatedRecord]
    job_id: str
    on_finish: Callable[["JobReducer[GenericAggregatedRecord]"], None]

    def __init__(
        self,
        comms: ReducerComms[GenericAggregatedRecord],
        config: Config,
        reducer: Reducer[GenericAggregatedRecord],
        job_id: str,
        on_finish: Callable[["JobReducer[GenericAggregatedRecord]"], None],
    ):
        super().__init__(State(set()))
        self.comms = comms
        self.reducer = reducer
        self.config = config
        self.job_id = job_id
        self.on_finish = on_finish

    def handle_aggregated(self, aggregated: GenericAggregatedRecord) -> None:
        self.reducer.handle_aggregated(aggregated)

    def handle_end(self, end: End) -> None:
        if end.host is None:
            logging.warn(f"Job {self.job_id} | Received End without host id")
            return
        self.state.ends_received.add(end.host)
        logging.debug(
            f"Job {self.job_id} | Aggregator {end.host} finished sending averages"
            f" ({len(self.state.ends_received)}/{self.config.aggregators_count})"
        )
        if len(self.state.ends_received) < self.config.aggregators_count:
            return

        StatePersistor().remove(self._control_store_key())
        StatePersistor().remove(self._joiner_store_key())
        self.on_finish(self)
        self.comms.send(self.job_id, self.reducer.get_value(), force_msg_id=None)

    def store_state(self) -> None:
        self.store_to(self._control_store_key())
        self.reducer.store_to(self._joiner_store_key())

    def restore_state(self) -> None:
        self.restore_from(self._control_store_key())
        self.reducer.restore_from(self._joiner_store_key())

    def _control_store_key(self) -> str:
        return f"control_{self.job_id}"

    def _joiner_store_key(self) -> str:
        return f"joiner_{self.job_id}"
