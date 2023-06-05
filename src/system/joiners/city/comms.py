from common.messages import RecordType
from common.messages.joined import JoinedCityTrip

from ..common.comms import JoinerComms
from .config import Config


class SystemCommunication(JoinerComms[JoinedCityTrip]):
    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        super().__init__(config)

    def _load_definitions(self) -> None:
        super()._load_definitions()
        self.__bind_city(self.other_queue, RecordType.STATION)
        self.__bind_city(self.trips_queue, RecordType.TRIP)

    def __bind_city(self, queue: str, record: RecordType) -> None:
        self.channel.queue_bind(queue, self.EXCHANGE, f"{record}.{self.config.city}.*")
