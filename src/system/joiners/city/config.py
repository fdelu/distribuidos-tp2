from ..common.config import Config as JoinerConfig

NAME = "city"


class Config(JoinerConfig):
    city: str

    def __init__(self) -> None:
        super().__init__(NAME)
        self.city = self.get("City").lower()
