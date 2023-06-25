from typing import Protocol

from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord
from common.persistence import WithStateProtocol


class Aggregator(
    WithStateProtocol, Protocol[GenericJoinedTrip, GenericAggregatedRecord]
):
    def handle_joined(self, trip: GenericJoinedTrip) -> None:
        ...

    def get_value(self) -> GenericAggregatedRecord | None:
        ...

    def reset(self) -> None:
        ...
