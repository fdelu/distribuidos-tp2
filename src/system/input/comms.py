from threading import Event, Thread
from typing import Callable

from common.comms_base import (
    SystemCommunicationBase,
    CommsSend,
    setup_job_queues,
    HeartbeatSender,
)
from common.messages.comms import Package
from common.messages.raw import RawRecord
from common.persistence import StatePersistor

from .config import Config

PENDING_KEY = "__pending_package"


class SystemCommunication(CommsSend[Package[RawRecord]], SystemCommunicationBase):
    pending_package: Package[RawRecord] | None = None
    stop_event: Event
    thread: Thread | None = None

    def __init__(self) -> None:
        super().__init__(Config())
        self.pending_package = StatePersistor().load(
            PENDING_KEY, Package[RawRecord] | None
        )
        self.stop_event = Event()
        HeartbeatSender(self, Config()).setup_timer()

    def _get_routing_details(self, msg: Package[RawRecord]) -> tuple[str, str]:
        return (
            Config().out_exchange,
            f"{msg.job_id}.{msg.messages[0].get_routing_key()}",
        )

    def setup_job_queue(self, job_id: str) -> None:
        self.__wait_until(
            lambda: setup_job_queues(
                self, Config().out_exchange, Config().out_batchs_queues, job_id
            )
        )

    def start(self) -> None:
        if self.thread is not None:
            raise RuntimeError("Already started")
        self.thread = Thread(target=self.run)
        self.thread.start()
        self.__send_pending(maybe_redelivered=True)

    def run(self) -> None:
        while not self.stop_event.is_set():
            self.connection.process_data_events(None)  # type: ignore

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.__wait_until(lambda: None)  # make process_data_events return
            self.thread.join()
            self.thread = None

    def send_msg(
        self, job_id: str, record: RawRecord, msg_id: str | None = None
    ) -> None:
        package = Package([record], msg_id, job_id)
        self.pending_package = package

    def flush(self) -> None:
        self.__save_state()
        self.__send_pending()

    def __save_state(self) -> None:
        StatePersistor().store(PENDING_KEY, self.pending_package)
        StatePersistor().save()

    def __send_pending(self, maybe_redelivered: bool = False) -> None:
        if self.pending_package is None:
            return
        package: Package[RawRecord] = self.pending_package
        package.maybe_redelivered = maybe_redelivered
        self.__wait_until(lambda: self.send(package))
        self.pending_package = None
        self.__save_state()

    def __wait_until(self, callback: Callable[[], None]) -> None:
        event = Event()

        def __wait_inner() -> None:
            callback()
            event.set()

        self.connection.add_callback_threadsafe(__wait_inner)
        event.wait()
