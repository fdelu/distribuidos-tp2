from typing import Callable, Generic, Protocol
import logging

from common.messages.aggregated import (
    GenericAggregatedRecordContr as GenericAggregatedRecord,
)
from common.messages.stats import StatsRecord
from common.messages import End, Message

from .config import Config
from .comms import ReducerComms


class Reducer(Protocol[GenericAggregatedRecord]):
    def handle_aggregated(self, aggregated: GenericAggregatedRecord) -> None:
        ...

    def get_value(self) -> StatsRecord:
        ...


class JobReducer(Generic[GenericAggregatedRecord]):
    comms: ReducerComms[GenericAggregatedRecord]
    config: Config
    reducer: Reducer[GenericAggregatedRecord]
    job_id: str
    ends_received: set[str]
    on_finish: Callable[["JobReducer[GenericAggregatedRecord]"], None]

    def __init__(
        self,
        comms: ReducerComms[GenericAggregatedRecord],
        config: Config,
        reducer: Reducer[GenericAggregatedRecord],
        job_id: str,
        on_finish: Callable[["JobReducer[GenericAggregatedRecord]"], None],
    ):
        self.comms = comms
        self.reducer = reducer
        self.ends_received = set()
        self.config = config
        self.job_id = job_id
        self.on_finish = on_finish

    def handle_aggregated(self, aggregated: GenericAggregatedRecord) -> None:
        self.reducer.handle_aggregated(aggregated)

    def handle_end(self, end: End) -> None:
        if end.host is None:
            logging.warn("Received End without host id")
            return
        self.ends_received.add(end.host)
        logging.debug(
            f"Aggregator {end.host} finished sending averages"
            f" ({len(self.ends_received)}/{self.config.aggregators_count})"
        )
        if len(self.ends_received) < self.config.aggregators_count:
            return

        self._send(self.reducer.get_value())
        self.on_finish(self)

    def handle_record(self, record: GenericAggregatedRecord | End) -> None:
        record.be_handled_by(self)

    def _send(self, record: StatsRecord) -> None:
        self.comms.send(Message(self.job_id, record))
