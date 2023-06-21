from typing import Protocol, TypeVar

from shared.messages import (
    RecordStart as RecordStartBase,
    LinesBatch as LinesBatchBase,
    AllSent as AllSentBase,
    Message as MessageBase,
)

T = TypeVar("T", covariant=True)

# Client Messages


class RecordStart(RecordStartBase):
    def be_handled_by(self, handler: "InputClientMessageHandler[T]") -> T:
        return handler.handle_start(self)


class LinesBatch(LinesBatchBase):
    def be_handled_by(self, handler: "InputClientMessageHandler[T]") -> T:
        return handler.handle_batch(self)


class AllSent(AllSentBase):
    def be_handled_by(self, handler: "InputClientMessageHandler[T]") -> T:
        return handler.handle_all_sent()


class InputClientMessageHandler(Protocol[T]):
    def handle_start(self, msg: RecordStart) -> T:
        ...

    def handle_batch(self, msg: LinesBatch) -> T:
        ...

    def handle_all_sent(self) -> T:
        ...


InputMessagePayloads = RecordStart | LinesBatch | AllSent
Message = MessageBase[InputMessagePayloads]
