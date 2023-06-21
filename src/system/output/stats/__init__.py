from threading import Lock

from shared.messages import Stat
from common.messages.stats import (
    StatType,
    StatsRecord,
)


class StatsStorage:
    stats: dict[str, dict[StatType, Stat]] = {}
    lock: Lock = Lock()

    def store(self, job_id: str, stat: StatsRecord) -> None:
        with self.lock:
            stats = self.stats.setdefault(job_id, {})
            stats[stat.stat_type()] = stat

    def get(self, job_id: str, stat_type: StatType) -> Stat | None:
        with self.lock:
            if job_id not in self.stats:
                return None
            return self.stats[job_id].get(stat_type, None)

    def all_done(self, job_id: str) -> bool:
        with self.lock:
            return job_id in self.stats and len(self.stats[job_id]) == len(StatType)
