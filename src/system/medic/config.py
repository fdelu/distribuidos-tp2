from common.config_base import ConfigBase
import os
from typing import cast


class Config(ConfigBase):
    address: str
    prefetch_count: int
    medic_id: int
    medic_scale: int

    def __init__(self) -> None:
        super().__init__()
        self.medic_id = cast(int, os.environ.get("ID"))
        self.medic_scale = cast(int, os.environ.get("MEDIC_SCALE"))
