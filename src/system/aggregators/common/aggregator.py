from typing import Protocol

from common.messages.joined import GenericJoinedTrip
from common.messages.aggregated import GenericAggregatedRecord


class Aggregator(Protocol[GenericJoinedTrip, GenericAggregatedRecord]):
    def handle_joined(self, trip: GenericJoinedTrip) -> None:
        ...

    def get_value(self) -> GenericAggregatedRecord:
        ...

    def reset(self) -> None:
        ...
