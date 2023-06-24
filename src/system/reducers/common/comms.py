from typing import Generic

from common.comms_base import ReliableComms, HeartbeatSender
from common.messages import End
from common.messages.aggregated import GenericAggregatedRecord
from common.messages.stats import StatsRecord

from .config import Config


class ReducerComms(
    Generic[GenericAggregatedRecord],
    ReliableComms[GenericAggregatedRecord | End, StatsRecord],
):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config, add_job_id_to_routing_key=False)
        HeartbeatSender(self, config).setup_timer()

    def _load_definitions(self) -> None:
        # in
        self._start_consuming_from(self.config.in_queue)

    def _get_routing_details(self, msg: StatsRecord) -> tuple[str, str]:
        return self.config.out_exchange, self.config.out_queue
