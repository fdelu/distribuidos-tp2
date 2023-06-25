from threading import Lock

from shared.messages import Stat
from common.messages.stats import (
    StatType,
    StatsRecord,
)
from common.persistence import WithState


# job_id -> stat_type -> stat
Stats = dict[str, dict[StatType, Stat]]


class StatsStorage(WithState[Stats]):
    lock: Lock = Lock()

    def __init__(self) -> None:
        super().__init__({})

    def store(self, job_id: str, stat: StatsRecord) -> None:
        with self.lock:
            stats = self.state.setdefault(job_id, {})
            stats[stat.stat_type()] = stat

    def get(self, job_id: str, stat_type: StatType) -> Stat | None:
        with self.lock:
            if job_id not in self.state:
                return None
            return self.state[job_id].get(stat_type, None)

    def all_done(self, job_id: str) -> bool:
        with self.lock:
            return job_id in self.state and len(self.state[job_id]) == len(StatType)

    def store_to(self, key: str) -> None:
        with self.lock:
            super().store_to(key)

    def load_from(self, key: str) -> None:
        with self.lock:
            super().restore_from(key)
