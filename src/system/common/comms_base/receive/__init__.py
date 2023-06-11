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

from shared.serde import deserialize, get_generic_types
from common.config_base import ConfigProtocol
from ..protocol import TIMEOUT_SECONDS, CommsProtocol

IN = TypeVar("IN")
STATUS_FILE = os.getenv("STATUS_FILE", "status.txt")


class ReceiveConfig(ConfigProtocol, Protocol):
    prefetch_count: int


class CommsReceive(CommsProtocol, Generic[IN], ABC):
    """
    Comms with receive capabilities. See protocol.py for more details about the methods.
    """

    __in_type: Type[Any] | None = None
    interrupted: Event = Event()
    stopped: Event = Event()

    callback: Callable[[IN], None] | None = None
    timeout_callbacks: dict[str, "TimeoutInfo"] = {}

    def __init__(self, config: ReceiveConfig, with_interrupt: bool = True) -> None:
        super().__init__(config)
        self.channel.basic_qos(prefetch_count=config.prefetch_count)
        self.callback = None
        if with_interrupt:
            self.__setup_interrupt()
        self.__setup()

    @property
    def in_type(self) -> Type[Any]:
        """
        Input type (resolved IN TypeVar from CommsReceive[IN])
        """
        if self.__in_type is None:
            self.__in_type = get_generic_types(self, CommsReceive)[0]
        return self.__in_type

    def start_consuming(self) -> None:
        """
        Start consuming messages from the queues
        """
        self.channel.start_consuming()

    def stop_consuming(self) -> None:
        """
        Signal/Thread-Safe way to stop consuming
        """
        self.connection.add_callback_threadsafe(self.__stop)

    def set_callback(self, callback: Callable[[IN], None]) -> None:
        """
        Sets the callback to be called when a record is received
        """
        self.callback = callback

    def set_timer(self, callback: Callable[[], None], timeout_seconds: float) -> Any:
        """
        Calls the callback after timeout seconds.
        Returns an object that can be used to cancel the timer.
        """
        logging.debug(f"Setting timer for {timeout_seconds} seconds")
        return self.connection.call_later(timeout_seconds, callback)

    def cancel_timer(self, timer: Any) -> None:
        """
        Cancels the timer
        """
        self.connection.remove_timeout(timer)

    def is_stopped(self) -> bool:
        """
        Returns whether the consuming was stopped (or will be soon)
        """
        return self.interrupted.is_set() or self.stopped.is_set()

    @abstractmethod
    def _load_definitions(self) -> None:
        """
        Declares the exchanges, queues and bindings required for the communication
        """
        ...

    def _start_consuming_from(self, queue: str) -> None:
        """
        Starts consuming from the given queue.
        This method returns when stop_consuming() is called.
        """
        callback = partial(self.__handle_record, queue)
        self.channel.basic_consume(queue=queue, on_message_callback=callback)

    def _set_timeout_callback(
        self,
        queue: str,
        callback: Callable[[], None],
        timeout: float = TIMEOUT_SECONDS,
    ) -> None:
        """
        Sets a callback to be called after timeout seconds passed since the last message
        of the given queue was received. If no message is received after the call to
        this method, the callback will be called after timeout seconds.
        """
        prev = self.timeout_callbacks.get(queue, None)
        if prev is not None:
            self.connection.remove_timeout(prev.timer)

        info = self.TimeoutInfo(callback, timeout, None)
        self.timeout_callbacks[queue] = info
        info.timer = self.set_timer(lambda: self.__timeout_handler(info), timeout)

    def _set_empty_queue_callback(
        self, queue: str, callback: Callable[[], None], **queue_kwargs: Any
    ) -> None:
        """
        Sets a callback to be called when the given queue is empty.
        """
        self._set_timeout_callback(
            queue,
            lambda: self.__check_messages_left(queue, callback, **queue_kwargs),
        )

    def _process_message(self, message: str) -> None:
        """
        Processes a message. Can be overridden by subclasses.
        """
        decoded = self.__deserialize_record(message)
        if self.callback is not None:
            self.callback(decoded)

    def __deserialize_record(self, message: str) -> IN:
        """
        Deserializes a record from the given message
        """
        return deserialize(self.in_type, message)  # type: ignore

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

        self._process_message(body.decode())

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
