from common.util import singleton

from ..common.config import Config as JoinerConfig

NAME = "year"


@singleton
class Config(JoinerConfig):
    year_base: int
    year_compared: int

    def __init__(self) -> None:
        super().__init__(NAME)
        self.year_base = self.get_int("YearBase")
        self.year_compared = self.get_int("YearCompared")
