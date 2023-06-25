from common.util import singleton

from ..common.config import Config as JoinerConfig

NAME = "rain"


@singleton
class Config(JoinerConfig):
    precipitation_threshold: float

    def __init__(self) -> None:
        super().__init__(NAME)
        self.precipitation_threshold = self.get_float("PrecipitationThreshold")
