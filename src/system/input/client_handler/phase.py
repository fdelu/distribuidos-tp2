from enum import StrEnum
import logging

from shared.messages import RecordStart, RecordType


class Phase(StrEnum):
    STATIONS_WEATHER = "stations_weather"
    TRIPS = "TRIPS"
    END = "END"


class PhaseValidator:
    job_id: str
    inner: Phase

    def __init__(self, job_id: str) -> None:
        self.inner = Phase.STATIONS_WEATHER
        self.job_id = job_id

    def current(self) -> Phase:
        return self.inner

    def validate_phase(self, message: RecordStart) -> bool:
        record_type = message.record_type
        if self.inner == Phase.END or (
            self.inner == Phase.TRIPS and record_type != RecordType.TRIP
        ):
            logging.error(
                f"Job {self.job_id} | Received {record_type} record in invalid phase"
                f" {self.inner}"
            )
            return False

        if self.inner == Phase.STATIONS_WEATHER and record_type == RecordType.TRIP:
            logging.info(f"Job {self.job_id} | Sending trips")
            self.inner = Phase.TRIPS

        return True
