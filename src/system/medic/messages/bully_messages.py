from dataclasses import dataclass


@dataclass
class ElectionMessage:
    id: int


@dataclass
class AnswerMessage:
    id: int


@dataclass
class CoordinatorMessage:
    id_coordinator: int


@dataclass
class AliveMessage:
    id: int


BullyMessage = ElectionMessage | AnswerMessage | CoordinatorMessage | AliveMessage