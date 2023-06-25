from dataclasses import dataclass


@dataclass
class State:
    starts_received: set[str]
    ends_received: set[str]
    count: int
    trips_phase: bool
