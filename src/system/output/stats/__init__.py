from threading import Lock
from typing import Any

from common.messages.stats import StatType, StatsRecord


class StatsStorage:
    stats: dict[str, dict[StatType, Any]] = {}
    lock: Lock = Lock()

    def store(self, job_id: str, stat: StatsRecord) -> None:
        with self.lock:
            self.stats.setdefault(job_id, {})
            self.stats[job_id][stat.stat_type()] = stat.data

    def get(self, job_id: str, stat_type: StatType) -> Any:
        with self.lock:
            if job_id not in self.stats:
                return None
            return self.stats[job_id].get(stat_type, None)

    def all_done(self, job_id: str) -> bool:
        with self.lock:
            return job_id in self.stats and all(
                x in self.stats[job_id] for x in StatType
            )
