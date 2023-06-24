from common.util import singleton

from ..common.config import Config as ConfigBase

NAME = "rain"


@singleton
class Config(ConfigBase):
    def __init__(self) -> None:
        super().__init__(NAME)
