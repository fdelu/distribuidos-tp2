import os
from typing import TypeVar, Any

from shared.serde import serialize, deserialize

PATH_BASE = "/state"
PATH_TEMP = os.path.join(PATH_BASE, "temp")
PATH_CURRENT = os.path.join(PATH_BASE, "current")
T = TypeVar("T")


class StatePersistor:
    # Key -> (value)
    in_progress: dict[str, Any]
    current: dict[str, str]

    def __new__(cls) -> "StatePersistor":
        if not hasattr(cls, "instance"):
            cls.instance = super(StatePersistor, cls).__new__(cls)
        return cls.instance

    def __init__(self) -> None:
        self.in_progress = {}

        if not os.path.isdir(PATH_BASE):
            os.mkdir(PATH_BASE)

        if not os.path.isfile(PATH_CURRENT):
            self.current = {}
            return

        with open(PATH_CURRENT, "r") as f:
            self.current = deserialize(dict[str, str], f.read())

    def store(self, key: str, value: T) -> None:
        """
        Stores the value with the given key.
        The value is commited on the next call to save().
        If the value is a reference type, the value at the
        time of save() is the one commited.
        """
        self.in_progress[key] = value

    def remove(self, key: str) -> None:
        """
        Removes the value with the given key.
        The value is commited on the next call to save().
        """
        self.in_progress.pop(key, None)

    def load(self, key: str, type: Any) -> Any | None:
        """
        Loads the last commited value with the given key.
        """
        if key not in self.current:
            return None

        return deserialize(type, self.current[key])

    def save(self) -> None:
        """
        Commits all stored values.
        """
        self.current = {x: serialize(y) for x, y in self.in_progress.items()}
        with open(PATH_TEMP, "w") as f:
            f.write(serialize(self.current))

        # os.rename is atomic
        os.rename(PATH_TEMP, PATH_CURRENT)
