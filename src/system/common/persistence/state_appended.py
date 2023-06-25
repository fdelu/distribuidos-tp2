from typing import Generic, TypeVar

from shared.serde import get_generic_types, deserialize, serialize

from .persistor import StatePersistor

K = TypeVar("K")
V = TypeVar("V")


class WithStateAppended(Generic[K, V]):
    state: dict[K, V]
    __pending: dict[K, V]

    def __init__(self) -> None:
        self.state = {}
        self.__pending = {}

    def set(self, key: K, value: V) -> None:
        self.state[key] = value
        self.__pending[key] = value

    def restore_from(self, key: str) -> None:
        key_type, value_type = get_generic_types(self, WithStateAppended)
        items = (
            deserialize(tuple[key_type, value_type], x)  # type: ignore
            for x in StatePersistor().iter(key)
        )
        self.state = {x: y for x, y in items}
        self.__pending = {}

    def store_to(self, store_key: str) -> None:
        if len(self.__pending) == 0:
            return

        for key, value in self.__pending.items():
            StatePersistor().append(store_key, serialize((key, value)))
        self.__pending = {}
