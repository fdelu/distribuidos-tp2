from threading import Event
import zmq

from shared.socket import SocketStopWrapper

from . import Comms
from ..config import BikeRidesAnalyzerConfig


class CommsOutput(Comms):
    def __init__(
        self,
        job_id: str,
        context: zmq.Context[zmq.Socket[None]],
        config: BikeRidesAnalyzerConfig,
        interrupt_event: Event,
    ) -> None:
        super().__init__()
        self.job_id = job_id
        socket = context.socket(zmq.REQ)
        socket.connect(config.output_address)
        self.socket = SocketStopWrapper(socket, interrupt_event)
