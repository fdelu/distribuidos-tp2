from threading import Event
import zmq

from shared.messages import ServerMessagesOutput
from shared.serde import deserialize

from . import Comms
from .socket import ClientSocket
from ..config import BikeRidesAnalyzerConfig


class CommsOutput(Comms):
    def __init__(
        self,
        job_id: str,
        context: zmq.Context[zmq.Socket[None]],
        config: BikeRidesAnalyzerConfig,
        interrupt_event: Event,
    ) -> None:
        socket = ClientSocket(context, config.output_address, interrupt_event)
        super().__init__(socket, job_id)

    def recv(self) -> ServerMessagesOutput:
        return deserialize(ServerMessagesOutput, self.socket.recv())
