import logging
from uuid import uuid4
from typing import Any, Generic

from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config
from .comms import AggregatorComms
from .aggregator import Aggregator


class TimerSender(Generic[GenericJoinedTrip, GenericAggregatedRecord]):
    job_id: str
    timer: Any
    comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord]
    aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
    config: Config
    aggregator_store_key: str

    def __init__(
        self,
        job_id: str,
        comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord],
        aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord],
        config: Config,
        aggregator_store_key: str,
    ):
        self.job_id = job_id
        self.comms = comms
        self.aggregator = aggregator
        self.config = config
        self.timer = None
        self.aggregator_store_key = aggregator_store_key

    def setup_timer(self) -> None:
        if self.timer is None:
            self.__set_timer()

    def remove_timer(self, flush: bool = True) -> None:
        if self.timer is not None:
            self.comms.cancel_timer(self.timer)
            self.timer = None
        if flush:
            self.__send_data()

    def __timer_callback(self) -> None:
        self.__send_data()
        self.__set_timer()

    def __send_data(self) -> None:
        data = self.aggregator.get_value()
        if data is None:
            logging.debug(f"Job {self.job_id} | No data to send")
            return

        logging.debug(f"Job {self.job_id} | Sending partial results")
        id = str(uuid4())
        self.aggregator.reset()
        self.aggregator.store_to(self.aggregator_store_key)

        self.comms.send(self.job_id, data, force_msg_id=id)

    def __set_timer(self) -> None:
        self.timer = self.comms.set_timer(
            self.__timer_callback, self.config.send_interval_seconds
        )
