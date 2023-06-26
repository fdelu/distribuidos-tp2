from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Event
import time
from typing import Callable, Protocol, TypeVar, Generic, Any
from functools import partial, cached_property
from signal import signal, SIGTERM

from pika import spec
from pika.adapters.blocking_connection import BlockingChannel

from shared.serde import deserialize, get_generic_types
from common.config_base import ConfigProtocol
from common.util import register_self_destruct

from ..protocol import TIMEOUT_SECONDS, CommsProtocol
from ..util import set_healthy

IN = TypeVar("IN")
T = TypeVar("T")


class ReceiveConfig(ConfigProtocol, Protocol):
    prefetch_count: int


class CommsReceive(CommsProtocol, Generic[IN], ABC):
    """
    Comms with receive capabilities. See protocol.py for more details about the methods.
    """

    interrupted: Event
    stopped: Event

    callback: Callable[[IN], None] | None = None
    timeout_callbacks: dict[str, "TimeoutInfo"]
    ctags: dict[str, str]

    def __init__(self, config: ReceiveConfig, with_interrupt: bool = True) -> None:
        super().__init__(config)
        self.channel.basic_qos(prefetch_count=config.prefetch_count)
        self.callback = None
        self.timeout_callbacks = {}
        self.interrupted = Event()
        self.stopped = Event()
        self.ctags = {}
        if with_interrupt:
            self.__setup_interrupt()
        self.__setup()

    @cached_property
    def in_type(self) -> Any:
        """
        Input type (resolved IN TypeVar from CommsReceive[IN])
        """
        return get_generic_types(self, CommsReceive)[0]

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
        # logging.debug(f"Setting timer for {timeout_seconds} seconds")
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
        Fails silently if the queue does not exist, logging a warning.
        """
        if queue in self.ctags:
            return

        callback = partial(self.__handle_record, queue)
        # Make sure others didn't delete it before I could consume
        self.channel.queue_declare(queue)

        self.ctags[queue] = self.channel.basic_consume(
            queue, on_message_callback=callback
        )

    def _stop_consuming_from(self, queue: str, delete_if_unused: bool = True) -> None:
        """
        Stops consuming from the given queue.
        Optionally deletes it if it's no longer used (no consumers and no messages).
        """
        # so that it doesn't fail if it doesn't exist
        self.channel.queue_declare(queue)
        if queue in self.ctags:
            self.channel.basic_cancel(self.ctags.pop(queue))

        if (
            delete_if_unused
            # Don't use passive=True in case two nodes cancel it's consumption
            # at the same time and one deletes before the other one declares it
            and self.channel.queue_declare(queue).method.consumer_count == 0
        ):
            self.channel.queue_delete(queue, if_empty=True)

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

        info = self.TimeoutInfo(queue, callback, timeout, None)
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

    def _process_message(
        self, message: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        """
        Processes a message. Can be overridden by subclasses.

        Deserializes the message and calls the callback if it's set.
        Should acknowledge the message.
        """
        decoded = deserialize(self.in_type, message)  # type: ignore
        if self.callback is not None:
            self.callback(decoded)

        if delivery_tag is not None:
            register_self_destruct("pre_ack")
            self.channel.basic_ack(delivery_tag)

    def _messages_left(self, queue: str) -> int | None:
        """
        Returns the number of messages left in the given queue
        """
        return self.channel.queue_declare(queue).method.message_count

    def __delete_queue_callback(self, queue: str) -> None:
        """
        Deletes the queue once it's empty
        """

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
        signal(SIGTERM, lambda *_: self.__interrupt())

    def __interrupt(self) -> None:
        """
        Interrupt handler
        """
        self.interrupted.set()
        self.stop_consuming()

    def __setup(self) -> None:
        """
        Loads the definitions. When done, writes "OK" to the status file.
        """
        self._load_definitions()
        set_healthy("OK")

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
        register_self_destruct("received_message")
        if self.interrupted.is_set():
            self.__stop()
            return

        timeout_info = self.timeout_callbacks.get(queue, None)
        if timeout_info is not None:
            timeout_info.last_message_on = time.time()

        self._process_message(
            body.decode(), queue, method.delivery_tag, method.redelivered
        )

    def __timeout_handler(self, info: "TimeoutInfo") -> None:
        """
        Handles a timeout for the given timeout info
        """
        if self.interrupted.is_set():
            self.__stop()
            return

        now = time.time()
        # logging.debug(
        #     f"Timeout handler | delay: {info.time_seconds}, last:"
        #     f" {info.last_message_on}, now: {now}"
        # )
        if info.last_message_on is None:
            info.callback()
            return

        seconds_remaining = info.time_seconds - (now - info.last_message_on)
        if seconds_remaining <= 0:
            info.callback()
            self.timeout_callbacks.pop(info.queue)
        else:
            info.timer = self.connection.call_later(
                seconds_remaining, lambda: self.__timeout_handler(info)
            )

    def __check_messages_left(self, queue: str, callback: Callable[[], None]) -> None:
        """
        Checks if there are messages left in the given queue. If not, calls the
        callback. If there are, calls _set_empty_queue_callback() again.
        """
        if self._messages_left(queue) == 0:
            callback()
            return

        # for some reason the queue is not empty but the timeout expired
        # set the timer again:
        self._set_empty_queue_callback(queue, callback)
        return

    @dataclass
    class TimeoutInfo:
        """
        Stores information about a timeout callback.
        See CommsReceive._set_timeout_callback()
        """

        queue: str
        callback: Callable[[], None]
        time_seconds: float  # timeout after this many seconds
        last_message_on: float | None  # time.time() when the last message was received
        timer: Any | None = None  # the timer object
