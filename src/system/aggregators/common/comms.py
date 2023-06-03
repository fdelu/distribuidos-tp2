from abc import abstractmethod
from uuid import uuid4
from typing import Callable, Protocol

from common.messages import RecordType, Message
from common.comms_base.protocol import CommsReceiveProtocol, CommsSendProtocol, IN, OUT

EXCHANGE = "{}_joined_records"
TRIPS_QUEUE = "{}_joined_trips"
END_QUEUE = "{}_aggregators_ends_" + str(uuid4())
OUT_QUEUE = "{}_aggregated"


class AggregatorComms(
    CommsReceiveProtocol[Message[IN]],
    CommsSendProtocol[Message[OUT]],
    Protocol[IN, OUT],
):
    @abstractmethod
    def set_all_trips_done_callback(self, callback: Callable[[], None]) -> None:
        ...


def load_definitions(comms: AggregatorComms[IN, OUT], name: str) -> tuple[str, str]:
    # in
    trips_queue = TRIPS_QUEUE.format(name)
    comms._start_consuming_from(trips_queue)

    end_queue = END_QUEUE.format(name)
    comms.channel.queue_declare(end_queue, exclusive=True)  # end
    comms.channel.queue_bind(end_queue, EXCHANGE.format(name), RecordType.END)
    comms._start_consuming_from(end_queue)
    return trips_queue, OUT_QUEUE.format(name)
