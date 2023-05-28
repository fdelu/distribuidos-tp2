from common.config_base import ConfigBase


class Config(ConfigBase):
    address: str

    def __init__(self):
        super().__init__("input")
        self.address = self.get("Address")
