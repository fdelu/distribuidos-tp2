from typing import Any, Concatenate, Protocol, TypeVar, ParamSpec, Callable
import json
import os
from configparser import ConfigParser, ExtendedInterpolation, DEFAULTSECT

CONFIG_PATH = "/config.ini"
SECTION_SEP = "."


T = TypeVar("T")
P = ParamSpec("P")


def check_subsections(
    func: Callable[Concatenate["ConfigBase", str, str, P], T]
) -> Callable[Concatenate["ConfigBase", str, P], T]:
    def get(self: "ConfigBase", key: str, *args: P.args, **kwds: P.kwargs) -> T:
        s = self.section
        while True:
            try:
                return func(self, s, key, *args, **kwds)
            except KeyError:
                if s == DEFAULTSECT:
                    raise

                if SECTION_SEP not in s:
                    s = DEFAULTSECT
                    continue

                s, _ = s.rsplit(SECTION_SEP, 1)

    return get


class ConfigProtocol(Protocol):
    rabbit_host: str
    log_level: str | None


class ConfigBase:
    parser: ConfigParser
    rabbit_host: str
    log_level: str | None

    section: str = DEFAULTSECT

    def __init__(self, section: str | None = None) -> None:
        if section is not None:
            self.section = section

        self.parser = ConfigParser(
            interpolation=ExtendedInterpolation(),
            empty_lines_in_values=False,
            default_section="",
        )
        self.parser.read(CONFIG_PATH)
        self.log_level = self.get("LogLevel", fallback=None)
        self.rabbit_host = self.get("RabbitHost")

    @staticmethod
    def subsection(section: str, subsection: str) -> str:
        return f"{section}{SECTION_SEP}{subsection}"

    @check_subsections
    def get(self, section: str, key: str, **kwargs: Any) -> str:
        return self.parser.get(section, key, vars=os.environ, **kwargs).strip()

    @check_subsections
    def get_int(self, section: str, key: str, **kwargs: Any) -> int:
        return self.parser.getint(section, key, vars=os.environ, **kwargs)

    @check_subsections
    def get_float(self, section: str, key: str, **kwargs: Any) -> float:
        return self.parser.getfloat(section, key, vars=os.environ, **kwargs)

    @check_subsections
    def get_bool(self, section: str, key: str, **kwargs: Any) -> bool:
        return self.parser.getboolean(section, key, vars=os.environ, **kwargs)

    @check_subsections
    def get_json(self, section: str, key: str, **kwargs: Any) -> Any:
        return json.loads(self.parser.get(section, key, vars=os.environ, **kwargs))
