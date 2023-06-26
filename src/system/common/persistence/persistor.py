import logging
import os
from typing import TypeVar, Any, Iterable
from enum import StrEnum
from shutil import rmtree, copytree

from shared.serde import serialize, deserialize
from common.util import singleton, register_self_destruct

STATUS_FILE = "/state_persistor_status.txt"
STATUS_FILE_TEMP = "/state_persistor_status_temp.txt"


class Status(StrEnum):
    Committed = "committed"
    Updating = "updating"


PATH_BASE = "/state"
PATH_BACKUP = os.path.join(PATH_BASE, "temp")
PATH_CURRENT = os.path.join(PATH_BASE, "current")
T = TypeVar("T")

LINES_BUFFER_SIZE = 1000


@singleton
class StatePersistor:
    pending: dict[str, Any]
    pending_append: dict[str, list[str]]
    pending_removed: set[str]

    def __init__(self) -> None:
        for path in (PATH_BASE, PATH_BACKUP, PATH_CURRENT):
            if not os.path.isdir(path):
                os.mkdir(path)

        if not os.path.isfile(STATUS_FILE):
            self.__set_status(Status.Committed)

        self.pending = {}
        self.pending_append = {}
        self.pending_removed = set()

        self.__restore()
        logging.info("Loaded state from disk")

    def store(self, key: str, value: T) -> None:
        """
        Stores the value with the given key.
        The value is Committed on the next call to save().
        If the value is a reference type, the value at the
        time of save() is the one Committed.
        """
        self.pending_removed.discard(key)
        self.pending[key] = value

    def append(self, key: str, value: str) -> None:
        """
        Append-mode storage.

        Appends the value with the given key.
        The value must not contain newlines.
        """
        self.pending_removed.discard(key)
        self.pending_append.setdefault(key, []).append(value)

    def iter(self, key: str) -> Iterable[str]:
        """
        Only for append-mode storage.

        Returns an iterator over the values with the given key.
        """
        path = os.path.join(PATH_CURRENT, key)
        if not os.path.isfile(path):
            return ()
        with open(path, "r") as f:
            lines = f.readlines(LINES_BUFFER_SIZE)
            while lines:
                for line in lines:
                    yield line.rstrip("\n")
                lines = f.readlines(LINES_BUFFER_SIZE)

    def remove(self, key: str) -> None:
        """
        Removes the value with the given key.
        The value is Committed on the next call to save().
        """
        self.pending.pop(key, None)
        self.pending_append.pop(key, None)
        self.pending_removed.add(key)

    def load(self, key: str, type: Any) -> Any | None:
        """
        Loads the last Committed value with the given key.
        """
        path = os.path.join(PATH_CURRENT, key)
        if not os.path.isfile(path):
            return None

        with open(path, "r") as f:
            return deserialize(type, f.read())

    def save(self) -> None:
        """
        Commits all stored values.
        """
        register_self_destruct("pre_save")
        self.__save_pending(PATH_BACKUP)
        self.__set_status(Status.Updating)
        register_self_destruct("mid_save")
        self.__save_pending(PATH_CURRENT)
        self.__set_status(Status.Committed)
        register_self_destruct("post_save")

        self.pending = {}
        self.pending_append = {}
        self.pending_removed = set()

    def __save_pending(self, to_path: str) -> None:
        for key, value in self.pending.items():
            path = os.path.join(to_path, key)
            with open(path, "w") as f:
                f.write(serialize(value))

        for key, lines in self.pending_append.items():
            path = os.path.join(to_path, key)
            with open(path, "a") as f:
                f.writelines(f"{v}\n" for v in lines)

        for key in self.pending_removed:
            path = os.path.join(to_path, key)
            if os.path.isfile(path):
                os.remove(path)

    def __set_status(self, status: Status) -> None:
        with open(STATUS_FILE_TEMP, "w") as f:
            f.write(status.value)

        # os.rename is atomic
        os.rename(STATUS_FILE_TEMP, STATUS_FILE)

    def __restore(self) -> None:
        with open(STATUS_FILE, "r") as f:
            status = Status(f.read())

        if status == Status.Committed:
            rmtree(PATH_BACKUP)
            copytree(PATH_CURRENT, PATH_BACKUP)
        elif status == Status.Updating:
            rmtree(PATH_CURRENT)
            copytree(PATH_BACKUP, PATH_CURRENT)
            self.__set_status(Status.Committed)
