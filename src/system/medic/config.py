from common.config_base import ConfigBase


class Config(ConfigBase):
    address: str
    prefetch_count: int
    medic_scale: int

    def __init__(self) -> None:
        super().__init__("medic")
        self.medic_scale = self.get_int("MEDIC_SCALE")
        self.prefetch_count = self.get_int("PrefetchCount")
