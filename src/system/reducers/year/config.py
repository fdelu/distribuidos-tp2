from ..common.config import Config as ConfigBase

NAME = "year"


class Config(ConfigBase):
    factor: float

    def __init__(self):
        super().__init__(NAME)
        self.factor = self.get_float("Factor")
