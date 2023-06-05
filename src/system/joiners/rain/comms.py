from common.messages import RecordType
from common.messages.joined import JoinedRainTrip

from ..common.comms import JoinerComms


class SystemCommunication(JoinerComms[JoinedRainTrip]):
    def _load_definitions(self) -> None:
        super()._load_definitions()
        self.channel.queue_bind(
            self.other_queue, self.EXCHANGE, f"{RecordType.WEATHER}.#"
        )
