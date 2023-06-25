from common.util import singleton

from ..common.config import Config as ConfigBase

NAME = "city"


@singleton
class Config(ConfigBase):
    min_distance_km: float

    def __init__(self) -> None:
        super().__init__(NAME)
        self.min_distance_km = self.get_float("MinDistanceKm")
