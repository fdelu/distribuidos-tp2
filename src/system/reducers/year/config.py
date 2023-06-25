from common.util import singleton

from ..common.config import Config as ConfigBase

NAME = "year"


@singleton
class Config(ConfigBase):
    factor: float

    def __init__(self) -> None:
        super().__init__(NAME)
        self.factor = self.get_float("Factor")
