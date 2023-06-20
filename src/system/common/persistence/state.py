from typing import Generic, TypeVar

from shared.serde import get_generic_types

from .persistor import StatePersistor

S = TypeVar("S")


class WithState(Generic[S]):
    state: S

    def __init__(self, default: S):
        self.state = default

    def restore_from(self, key: str) -> None:
        state_type = get_generic_types(self, WithState)[0]
        state = StatePersistor().load(key, state_type)
        if state is not None:
            self.state = state

    def store_to(self, key: str) -> None:
        StatePersistor().store(key, self.state)
