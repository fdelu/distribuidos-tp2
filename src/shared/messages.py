from enum import StrEnum


class Phase(StrEnum):
    StationsWeather = "stations_weather"
    Trips = "trips"
    End = "end"


class SplitChar(StrEnum):
    HEADER = "|"
    ATTRS = ","
    RECORDS = "\n"


class StatType(StrEnum):
    RAIN = "rain"
    YEAR = "year"
    CITY = "city"


class RecordType(StrEnum):
    STATION = "station"
    TRIP = "trip"
    WEATHER = "weather"
    END = "end"
    ACK = "ack"
