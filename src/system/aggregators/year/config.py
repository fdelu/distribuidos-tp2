from ..common.config import Config as ConfigBase

NAME = "year"


class Config(ConfigBase):
    year_base: str
    year_compared: str

    def __init__(self):
        super().__init__(NAME)
        self.year_base = self.get("YearBase")
        self.year_compared = self.get("YearCompared")
