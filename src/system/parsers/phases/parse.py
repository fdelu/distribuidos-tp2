from typing import Iterable
from datetime import date, timedelta

from shared.messages import SplitChar
from common.messages.basic import BasicStation, BasicTrip, BasicWeather
from common.messages.raw import RawBatch


def parse_optional_float(value: str) -> float | None:
    if value == "":
        return None
    return float(value)


def get_indexes(batch: RawBatch) -> dict[str, int]:
    return {x: i for i, x in enumerate(batch.headers.split(SplitChar.ATTRS))}


def get_rows(batch: RawBatch) -> Iterable[list[str]]:
    return (x.split(SplitChar.ATTRS) for x in batch.lines)


def parse_station(row: list[str], indexes: dict[str, int], city: str) -> BasicStation:
    return BasicStation(
        code=row[indexes["code"]],
        name=row[indexes["name"]],
        latitude=parse_optional_float(row[indexes["latitude"]]),
        longitude=parse_optional_float(row[indexes["longitude"]]),
        year=row[indexes["yearid"]],
        city=city,
    )


def parse_trip(row: list[str], indexes: dict[str, int], city: str) -> BasicTrip:
    start_date = row[indexes["start_date"]].split(" ")[0]
    return BasicTrip(
        start_date=start_date,
        duration_sec=max(float(row[indexes["duration_sec"]]), 0),
        city=city,
        start_station_code=row[indexes["start_station_code"]],
        end_station_code=row[indexes["end_station_code"]],
        year=row[indexes["yearid"]],
    )


def parse_weather(row: list[str], indexes: dict[str, int], city: str) -> BasicWeather:
    day_minus_1 = date.fromisoformat(row[indexes["date"]]) - timedelta(days=1)
    return BasicWeather(
        date=day_minus_1.isoformat(),
        precipitation=float(row[indexes["prectot"]]),
        city=city,
    )
