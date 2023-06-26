from typing import Any, Protocol, TypeVar, Callable
import json
import os
from configparser import ConfigParser, ExtendedInterpolation, DEFAULTSECT, NoOptionError

CONFIG_PATH = "/config.ini"
SECTION_SEP = "."


T = TypeVar("T")
S = TypeVar("S")


class _Unset:
    pass


def check_subsections(
    section: str,
    func: Callable[[str], S],
    fallback: T | _Unset,
) -> T | S:
    while True:
        try:
            return func(section)
        except (KeyError, NoOptionError):
            if section == DEFAULTSECT:
                if not isinstance(fallback, _Unset):
                    return fallback
                raise

            if SECTION_SEP not in section:
                section = DEFAULTSECT
                continue

            section, _ = section.rsplit(SECTION_SEP, 1)


class ConfigProtocol(Protocol):
    rabbit_host: str
    log_level: str | None


class ConfigBase:
    parser: ConfigParser
    rabbit_host: str
    log_level: str | None
    system_name: str

    heartbeat_exchange: str
    heartbeat_frequency: float
    heartbeat_routing_key: str

    section: str = DEFAULTSECT

    def __init__(self, section: str) -> None:
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
        self.heartbeat_exchange = self.get("HeartbeatExchange")
        self.heartbeat_frequency = self.get_float("HeartbeatFrequency")
        self.heartbeat_routing_key = self.get("HeartbeatRoutingKey")
        self.system_name = self.get("SystemName")

    @staticmethod
    def subsection(section: str, subsection: str) -> str:
        return f"{section}{SECTION_SEP}{subsection}"

    def get_subsections(self, section: str) -> list[str]:
        """
        Returns the names of the subsections of the given section.
        """
        prefix = f"{section}{SECTION_SEP}"
        return [
            s[len(prefix) :] for s in self.parser.sections() if s.startswith(prefix)
        ]

    def get(
        self,
        key: str,
        section: str | None = None,
        fallback: T | _Unset = _Unset(),
    ) -> str | T:
        if section is None:
            section = self.section
        return check_subsections(
            section,
            lambda s: self.parser.get(s, key, vars=os.environ).strip(),
            fallback,
        )

    def get_int(
        self, key: str, section: str | None = None, fallback: T | _Unset = _Unset()
    ) -> int | T:
        if section is None:
            section = self.section
        return check_subsections(
            section,
            lambda s: self.parser.getint(s, key, vars=os.environ),
            fallback,
        )

    def get_float(
        self, key: str, section: str | None = None, fallback: T | _Unset = _Unset()
    ) -> float | T:
        if section is None:
            section = self.section
        return check_subsections(
            section,
            lambda s: self.parser.getfloat(s, key, vars=os.environ),
            fallback,
        )

    def get_bool(
        self, key: str, section: str | None = None, fallback: T | _Unset = _Unset()
    ) -> bool | T:
        if section is None:
            section = self.section
        return check_subsections(
            section,
            lambda s: self.parser.getboolean(s, key, vars=os.environ),
            fallback,
        )

    def get_json(
        self, key: str, section: str | None = None, fallback: T | _Unset = _Unset()
    ) -> Any | T:
        result = self.get(key, section=section, fallback=fallback)
        if result is fallback:
            return result
        return json.loads(result)  # type: ignore
