import logging
import zmq

from shared.log import setup_logs
from common.util import process_loop

from .input_server import InputServer
from .comms import SystemCommunication
from .config import Config


class Runner:
    context: zmq.Context[zmq.Socket[None]]
    input_server: InputServer
    comms: SystemCommunication

    def __init__(self) -> None:
        self.context = zmq.Context[zmq.Socket[None]]()
        self.context.setsockopt(zmq.LINGER, 0)  # Don't block on close
        self.comms = SystemCommunication()
        self.input_server = InputServer(self.context, self.comms)

    def run(self) -> None:
        self.comms.start()
        self.input_server.run()

    def cleanup(self) -> None:
        self.comms.stop()
        self.input_server.cleanup()
        self.comms.close()
        self.context.term()


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(lambda: Runner())

    logging.info("Exiting gracefully")


main()
