import logging
from uuid import uuid4
from typing import Any, Generic

from common.messages import Message
from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord

from .config import Config
from .comms import AggregatorComms
from .aggregator import Aggregator


class TimerSender(Generic[GenericJoinedTrip, GenericAggregatedRecord]):
    job_id: str
    timer: Any
    last: tuple[str, Message[GenericAggregatedRecord]] | None
    comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord]
    aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord]
    config: Config

    def __init__(
        self,
        job_id: str,
        comms: AggregatorComms[GenericJoinedTrip, GenericAggregatedRecord],
        aggregator: Aggregator[GenericJoinedTrip, GenericAggregatedRecord],
        config: Config,
    ):
        self.job_id = job_id
        self.comms = comms
        self.aggregator = aggregator
        self.config = config
        self.timer = None
        self.last = None

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
        logging.debug(f"Job {self.job_id} | Sending partial results")
        msg = Message(self.job_id, self.aggregator.get_value())
        id = str(uuid4())
        self.last = (id, msg)
        self.aggregator.reset()
        self.comms.send(msg, force_msg_id=id)

    def __set_timer(self) -> None:
        self.timer = self.comms.set_timer(
            self.__timer_callback, self.config.send_interval_seconds
        )
