from common.messages import RecordType
from common.messages.joined import JoinedYearTrip

from ..common.comms import JoinerComms
from .config import Config


class SystemCommunication(JoinerComms[JoinedYearTrip]):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        super()._load_definitions()
        self.__bind_years(self.other_queue, RecordType.STATION)
        self.__bind_years(self.trips_queue, RecordType.TRIP)

    def __bind_years(self, queue: str, record: RecordType) -> None:
        for year in (self.config.year_base, self.config.year_compared):
            self.channel.queue_bind(queue, self.EXCHANGE, f"{record}.*.{year}")
