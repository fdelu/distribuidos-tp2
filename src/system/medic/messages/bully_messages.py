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

    def be_handled_by(self, handler: "AnswerMessageHandler[T]") -> T:
        return handler.handle_answer(self)


class AnswerMessageHandler(Protocol[T]):
    def handle_answer(self, election_message: AnswerMessage) -> T:
        ...


@dataclass
class CoordinatorMessage:
    id_coordinator: int

    def be_handled_by(self, handler: "CoordinatorMessageHandler[T]") -> T:
        return handler.handle_coordinator(self)


class CoordinatorMessageHandler(Protocol[T]):
    def handle_coordinator(self, election_message: CoordinatorMessage) -> T:
        ...


@dataclass
class AliveMessage:
    id: int

    def be_handled_by(self, handler: "AliveMessageHandler[T]") -> T:
        return handler.handle_alive(self)


class AliveMessageHandler(Protocol[T]):
    def handle_alive(self, election_message: AliveMessage) -> T:
        ...


BullyMessage = ElectionMessage | AnswerMessage | CoordinatorMessage | AliveMessage
