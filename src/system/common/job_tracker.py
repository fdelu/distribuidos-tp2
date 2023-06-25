from dataclasses import dataclass
from typing import TypeVar, Callable
import logging

from common.persistence.state import WithState


JOB_TRACKER_KEY = "__job_tracker"

T = TypeVar("T")


@dataclass
class Jobs:
    in_progress: set[str]
    completed: set[str]


class JobTracker(WithState[Jobs]):
    def __init__(self) -> None:
        super().__init__(Jobs(set(), set()))
        self.restore_from(JOB_TRACKER_KEY)

    def finished_job(self, job_id: str) -> None:
        self.state.in_progress.discard(job_id)
        self.state.completed.add(job_id)
        self.store_to(JOB_TRACKER_KEY)

    def start_job(self, job_id: str) -> None:
        self.state.in_progress.add(job_id)
        self.store_to(JOB_TRACKER_KEY)

    def restore(self, jobs: dict[str, T], factory: Callable[[str], T]) -> None:
        for job_id in self.state.in_progress:
            jobs[job_id] = factory(job_id)
        if len(self.state.in_progress) > 0:
            logging.info(f"Restored {len(self.state.in_progress)} jobs")
