from abc import ABC, abstractmethod
from dataclasses import dataclass
import logging
from threading import Event
import time
from typing import Callable, Protocol, TypeVar, Generic, Any, Type
import os
from functools import partial
from signal import signal, SIGTERM

from pika import spec
from pika.adapters.blocking_connection import BlockingChannel

from common.messages import Batch
from common.serde import deserialize, get_generic_types
from common.config_base import ConfigProtocol
from ..protocol import TIMEOUT_SECONDS, CommsProtocol

from .reliable import RealiableReceive

IN = TypeVar("IN")
STATUS_FILE = os.getenv("STATUS_FILE", "status.txt")


class ReceiveConfig(ConfigProtocol, Protocol):
    prefetch_count: int


class CommsReceive(CommsProtocol, Generic[IN], ABC):
    """
    Comms with receive capabilities. See protocol.py for more details about the methods.
    """

    in_type: Type[Any] | None = None
    interrupted: Event = Event()
    stopped: Event = Event()
    reliable_handler: RealiableReceive | None = None

    callback: Callable[[IN], None] | None = None
    timeout_callbacks: dict[str, "TimeoutInfo"] = {}
    batch_done_callback: Callable[[], None] | None = None

    def __init__(
        self, config: ReceiveConfig, with_interrupt: bool = True, reliable: bool = True
    ) -> None:
        super().__init__(config)
        self.channel.basic_qos(prefetch_count=config.prefetch_count)
        self.callback = None
        if with_interrupt:
            self.__setup_interrupt()
        self.__setup()

        if reliable:
            self.reliable_handler = RealiableReceive()

    def start_consuming(self) -> None:
        self.channel.start_consuming()

    def stop_consuming(self) -> None:
        self.connection.add_callback_threadsafe(self.__stop)

    def set_callback(self, callback: Callable[[IN], None]) -> None:
        self.callback = callback

    def set_batch_done_callback(self, callback: Callable[[], None]) -> None:
        self.batch_done_callback = callback

    def set_timer(self, callback: Callable[[], None], timeout_seconds: float) -> Any:
        return self.connection.call_later(timeout_seconds, callback)

    def cancel_timer(self, timer: Any) -> None:
        self.connection.remove_timeout(timer)

    def is_stopped(self) -> bool:
        return self.interrupted.is_set() or self.stopped.is_set()

    def current_message_id(self) -> str | None:
        if self.reliable_handler:
            return self.reliable_handler.current_message_id()
        return None

    @abstractmethod
    def _load_definitions(self) -> None:
        ...

    def _start_consuming_from(self, queue: str) -> None:
        callback = partial(self.__handle_record, queue)
        self.channel.basic_consume(queue=queue, on_message_callback=callback)

    def _set_timeout_callback(
        self,
        queue: str,
        callback: Callable[[], None],
        timeout: float = TIMEOUT_SECONDS,
    ) -> None:
        prev = self.timeout_callbacks.get(queue, None)
        if prev is not None:
            self.connection.remove_timeout(prev.timer)

        info = self.TimeoutInfo(callback, timeout or TIMEOUT_SECONDS, None)
        self.timeout_callbacks[queue] = info
        info.timer = self.connection.call_later(
            info.time_seconds, lambda: self.__timeout_handler(info)
        )

    def _set_empty_queue_callback(
        self, queue: str, callback: Callable[[], None], **queue_kwargs: Any
    ) -> None:
        self._set_timeout_callback(
            queue,
            lambda: self.__check_messages_left(queue, callback, **queue_kwargs),
        )

    def __deserialize_record(self, message: str) -> Batch[IN]:
        if self.in_type is None:
            self.in_type = get_generic_types(self, CommsReceive)[0]
        return deserialize(Batch[self.in_type], message)  # type: ignore

    def __stop(self) -> None:
        """
        Stops consuming and sets the stopped flag
        """
        self.channel.stop_consuming()
        self.stopped.set()
        if self.interrupted.is_set():
            return

    def __setup_interrupt(self) -> None:
        """
        Sets up the interrupt handler
        """
        signal(SIGTERM, lambda *_: self.stop_consuming())

    def __setup(self) -> None:
        """
        Loads the definitions. When done, writes "OK" to the status file.
        """
        self._load_definitions()
        with open(STATUS_FILE, "w") as f:
            f.write("OK")

    def __handle_record(
        self,
        queue: str,
        ch: BlockingChannel,
        method: spec.Basic.Deliver,
        _props: spec.BasicProperties,
        body: bytes,
    ) -> None:
        """
        Handles a message received from the queue, calling the callback
        if set and acknowledging the message afterwards.
        This method returns when stop_consuming() is called.
        """
        if self.interrupted.is_set():
            self.__stop()
            return

        timeout_info = self.timeout_callbacks.get(queue, None)

        if timeout_info is not None:
            timeout_info.last_message_on = time.time()

        batch = self.__deserialize_record(body.decode())

        if not self.reliable_handler or self.reliable_handler.pre_process(batch):
            if self.callback is not None:
                for msg in batch.messages:
                    self.callback(msg)
            if self.batch_done_callback:
                self.batch_done_callback()
            if self.reliable_handler:
                self.reliable_handler.post_process()

        if method.delivery_tag is not None:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def __timeout_handler(self, info: "TimeoutInfo") -> None:
        """
        Handles a timeout for the given timeout info
        """
        if self.interrupted.is_set():
            self.__stop()
            return

        now = time.time()
        logging.debug(
            f"Timeout handler | delay: {info.time_seconds}, last:"
            f" {info.last_message_on}, now: {now}"
        )
        if info.last_message_on is None:
            info.callback()
            return

        seconds_remaining = info.time_seconds - (now - info.last_message_on)
        if seconds_remaining <= 0:
            info.callback()
        else:
            info.timer = self.connection.call_later(
                seconds_remaining, lambda: self.__timeout_handler(info)
            )

    def __check_messages_left(
        self, queue: str, callback: Callable[[], None], **queue_kwargs: Any
    ) -> None:
        """
        Checks if there are messages left in the given queue. If not, calls the
        callback. If there are, calls _set_empty_queue_callback() again.
        """
        res = self.channel.queue_declare(queue=queue, passive=True, **queue_kwargs)
        messages_left = res.method.message_count
        if messages_left == 0:
            callback()
        else:
            # for some reason the queue is not empty but the timeout expired
            # set the timer again:
            self._set_empty_queue_callback(queue, callback, **queue_kwargs)

    @dataclass
    class TimeoutInfo:
        """
        Stores information about a timeout callback.
        See CommsReceive._set_timeout_callback()
        """

        callback: Callable[[], None]
        time_seconds: float  # timeout after this many seconds
        last_message_on: float | None  # time.time() when the last message was received
        timer: Any | None = None  # the timer object
