from common.config_base import ConfigBase
import os
from typing import cast


class Config(ConfigBase):
    address: str
    prefetch_count: int
    abs_path: str

    def __init__(self) -> None:
        super().__init__()
        self.abs_path = cast(str, os.environ.get("ABS_PATH"))

    def get_env(self, key: str) -> str:
        return cast(str, os.environ.get(key))
