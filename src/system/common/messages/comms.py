from dataclasses import dataclass
from typing import Generic, TypeVar, Protocol

T = TypeVar("T", covariant=True)
S = TypeVar("S", contravariant=True)


@dataclass
class Package(Generic[T]):
    messages: list[T]
    msg_id: str | None
    job_id: str
    maybe_redelivered: bool = False


@dataclass
class CheckProcessed:
    check_id: str
    job_id: str
    msg_id: str
    host_id: str

    def get_routing_key(self) -> str:
        return "check_processed"


@dataclass
class CheckProcessedResponse:
    check_id: str
    job_id: str
    msg_id: str
    processed: bool
    host_id: str

    def get_routing_key(self) -> str:
        return "check_processed_response"


class PackageHandler(Protocol[S]):
    def handle_package(self, package: Package[S], delivery_tag: int | None) -> None:
        ...


CommsMessage = Package[T] | CheckProcessed | CheckProcessedResponse
