from typing import Protocol
import os
from configparser import ConfigParser, ExtendedInterpolation, DEFAULTSECT

CONFIG_PATH = "/config.ini"


class ConfigProtocol(Protocol):
    rabbit_host: str
    log_level: str | None


class ConfigBase:
    parser: ConfigParser
    rabbit_host: str
    log_level: str | None

    section: str = DEFAULTSECT

    def __init__(self, section: str | None = None):
        if section is not None:
            self.section = section

        self.parser = ConfigParser(interpolation=ExtendedInterpolation())
        self.parser.read(CONFIG_PATH)
        self.log_level = self.get("LogLevel", fallback=None)
        self.rabbit_host = self.get("RabbitHost")

    def get(self, key: str, **kwargs) -> str:
        return self.parser.get(self.section, key, vars=os.environ, **kwargs).strip()

    def get_int(self, key: str, **kwargs) -> int:
        return self.parser.getint(self.section, key, vars=os.environ, **kwargs)

    def get_float(self, key: str, **kwargs) -> float:
        return self.parser.getfloat(self.section, key, vars=os.environ, **kwargs)

    def get_bool(self, key: str, **kwargs) -> bool:
        return self.parser.getboolean(self.section, key, vars=os.environ, **kwargs)
