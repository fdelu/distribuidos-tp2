from typing import Protocol, TypeVar
from dataclasses import dataclass


T = TypeVar("T", covariant=True)


@dataclass
class ElectionMessage:
    id: int

    def be_handled_by(self, handler: "ElectionMessageHandler[T]") -> T:
        return handler.handle_election(self)


class ElectionMessageHandler(Protocol[T]):
    def handle_election(self, election_message: ElectionMessage) -> T:
        ...


@dataclass
class AnswerMessage:
    id: int
    receiver_id: int

    def be_handled_by(self, handler: "AnswerMessageHandler[T]") -> T:
        return handler.handle_answer(self)


class AnswerMessageHandler(Protocol[T]):
    def handle_answer(self, awnser_message: AnswerMessage) -> T:
        ...


@dataclass
class CoordinatorMessage:
    id_coordinator: int

    def be_handled_by(self, handler: "CoordinatorMessageHandler[T]") -> T:
        return handler.handle_coordinator(self)


class CoordinatorMessageHandler(Protocol[T]):
    def handle_coordinator(self, coordinator_message: CoordinatorMessage) -> T:
        ...


@dataclass
class AliveMessage:
    container_name: str

    def be_handled_by(self, handler: "AliveMessageHandler[T]") -> T:
        return handler.handle_alive(self)


class AliveMessageHandler(Protocol[T]):
    def handle_alive(self, alive_message: AliveMessage) -> T:
        ...


@dataclass
class AliveLeaderMessage:
    id: int

    def be_handled_by(self, handler: "AliveLeaderMessageHandler[T]") -> T:
        return handler.handle_alive_leader(self)


class AliveLeaderMessageHandler(Protocol[T]):
    def handle_alive_leader(self, alive_leader_message: AliveLeaderMessage) -> T:
        ...


BullyMessage = ElectionMessage | AnswerMessage | CoordinatorMessage | AliveMessage \
    | AliveLeaderMessage
