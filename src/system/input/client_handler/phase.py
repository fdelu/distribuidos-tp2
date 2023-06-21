from enum import StrEnum
import logging

from shared.messages import RecordStart, RecordType


class Phases(StrEnum):
    STATIONS_WEATHER = "stations_weather"
    TRIPS = "TRIPS"
    END = "END"


class Phase:
    job_id: str
    inner: Phases

    def __init__(self, job_id: str) -> None:
        self.inner = Phases.STATIONS_WEATHER
        self.job_id = job_id

        logging.info(f"Job {job_id} | Receiving weather & stations")

    def validate_phase(self, message: RecordStart) -> bool:
        record_type = message.record_type
        if self.inner == Phases.END or (
            self.inner == Phases.TRIPS and record_type != RecordType.TRIP
        ):
            logging.error(
                f"Job {self.job_id} | Received {record_type} record in invalid phase"
                f" {self.inner}"
            )
            return False

        if self.inner == Phases.STATIONS_WEATHER and record_type == RecordType.TRIP:
            logging.info(f"Job {self.job_id} | Sending trips")
            self.inner = Phases.TRIPS

        return True
