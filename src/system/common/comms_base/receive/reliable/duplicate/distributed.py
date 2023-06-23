from typing import Generic, Protocol
from dataclasses import dataclass
from functools import singledispatchmethod
import logging

from shared.serde import deserialize, serialize
from common.messages.comms import (
    Package,
    CommsMessage,
    CheckProcessed,
    CheckProcessedResponse,
)
from common.persistence import StatePersistor

from . import DuplicateFilter, IN, RECEIVED_MESSAGES_KEY, PackageComms

__all__ = ["DuplicateFilter"]


class FilterConfig(Protocol):
    filters_exchange: str
    in_others_queue_format: str
    filters_routing_keys_format: list[str]
    host_count: int


@dataclass
class PendingCheck(Generic[IN]):
    queue: str
    package: Package[IN]
    responses: set[str]
    delivery_tag: int | None


class DuplicateFilterDistributed(DuplicateFilter[IN], Generic[IN]):
    config: FilterConfig
    # msg_id -> PendingCheck
    pending_checks: dict[str, PendingCheck[IN]]

    def __init__(self, package_handler: PackageComms[IN], config: FilterConfig) -> None:
        super().__init__(package_handler)
        self.config = config
        self.pending_checks = {}

    def load_definitions(self) -> None:
        for i in range(1, self.config.host_count + 1):
            q = self.config.in_others_queue_format.format(host_id=i)
            for rk in self.config.filters_routing_keys_format:
                self.comms.channel.queue_bind(
                    q, self.config.filters_exchange, rk.format(host_id=i)
                )

    def received_message(
        self, data: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        message = self.__deserialize_package(data)
        self.handle_message(message, queue, delivery_tag, redelivered)

    def pending_count(self, queue: str) -> int:
        return sum(1 for c in self.pending_checks.values() if c.queue == queue)

    @singledispatchmethod
    def handle_message(
        self,
        message: CommsMessage[IN],
        queue: str,
        delivery_tag: int | None,
        redelivered: bool,
    ) -> None:
        raise NotImplementedError("Unknown message type")

    @handle_message.register(Package)
    def handle_package(
        self,
        package: Package[IN],
        queue: str,
        delivery_tag: int | None,
        redelivered: bool,
    ) -> None:
        if not package.msg_id or not (redelivered or package.maybe_redelivered):
            self.__process(package, delivery_tag)
            return

        if self.__processed_local(package):
            logging.warn(
                f"Received package {package.msg_id} already processed locally,"
                " acknowledging"
            )
            self._ack(delivery_tag)
            return
        elif self.config.host_count == 1:
            # this is the only node and it wasn't processed here
            self.__process(package, delivery_tag)
            return

        logging.warn(
            f"Received package {package.msg_id} that may have already been processed,"
            " sending check to other nodes"
        )
        self.__send_check(package, queue, delivery_tag)

    @handle_message.register
    def handle_check_processed(
        self,
        check: CheckProcessed,
        queue: str,
        delivery_tag: int | None,
        redelivered: bool,
    ) -> None:
        if check.host_id == self.comms.id:
            self._ack(delivery_tag)
            return

        processed = check.msg_id in self.received_messages
        logging.debug(
            f"Received CheckProcessed from host {check.host_id} for {check.msg_id}."
            f" Processed locally: {processed}"
        )
        response = CheckProcessedResponse(check.msg_id, processed, self.comms.id)
        self.comms.channel.basic_publish(
            self.config.filters_exchange,
            f"{response.get_routing_key()}.{check.host_id}",
            serialize(response).encode(),
        )
        self._ack(delivery_tag)

    @handle_message.register
    def handle_check_processed_response(
        self,
        response: CheckProcessedResponse,
        queue: str,
        delivery_tag: int | None,
        redelivered: bool,
    ) -> None:
        if response.msg_id not in self.pending_checks:
            self._ack(delivery_tag)
            return

        check = self.pending_checks[response.msg_id]
        if response.processed:
            logging.warn(
                f"Check finished: Package {check.package.msg_id} had been processed by"
                f" {response.host_id}, acknowledging"
            )
            self.pending_checks.pop(response.msg_id)
            self._ack(check.delivery_tag)
            self._ack(delivery_tag)
            return

        check.responses.add(response.host_id)
        logging.debug(
            f"Received negative CheckProcessedResponse from host {response.host_id} for"
            f" {response.msg_id} ({len(check.responses)}/{self.config.host_count - 1})"
        )
        if len(check.responses) < self.config.host_count - 1:
            self._ack(delivery_tag)
            return

        logging.warn(
            f"Check finished: Package {check.package.msg_id} hadn't been processed by"
            " any other node, processing locally"
        )
        check = self.pending_checks.pop(response.msg_id)
        self.__process(check.package, check.delivery_tag)
        self._ack(delivery_tag)

    def __send_check(
        self, msg: Package[IN], queue: str, delivery_tag: int | None
    ) -> None:
        if not msg.msg_id or msg.msg_id in self.pending_checks:
            return

        self.pending_checks[msg.msg_id] = PendingCheck(queue, msg, set(), delivery_tag)
        check = CheckProcessed(msg.msg_id, self.comms.id)
        self.comms.channel.basic_publish(
            self.config.filters_exchange,
            check.get_routing_key(),
            serialize(check).encode(),
        )

    def __processed_local(self, package: Package[IN]) -> bool:
        return package.msg_id is not None and package.msg_id in self.received_messages

    def __deserialize_package(self, message: str) -> CommsMessage[IN]:
        return deserialize(CommsMessage[self.comms.in_type], message)  # type: ignore

    def __process(self, package: Package[IN], delivery_tag: int | None) -> None:
        if package.msg_id:
            self.received_messages.add(package.msg_id)
            StatePersistor().store(RECEIVED_MESSAGES_KEY, self.received_messages)
        self.comms.handle_package(package, delivery_tag)
