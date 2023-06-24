import logging
import zmq

from shared.log import setup_logs
from common.util import process_loop

from .stats import StatsStorage
from .stats.receiver import StatsReceiver
from .handler import ClientHandler
from .config import Config


class Runner:
    context: zmq.Context[zmq.Socket[None]]
    client_handler: ClientHandler
    stats_receiver: StatsReceiver

    def __init__(self) -> None:
        self.context = zmq.Context()
        self.context.setsockopt(zmq.LINGER, 0)  # Don't block on close
        stats = StatsStorage()
        self.client_handler = ClientHandler(self.context, stats)
        self.stats_receiver = StatsReceiver(stats)

    def run(self) -> None:
        self.client_handler.start()
        self.stats_receiver.run()

    def cleanup(self) -> None:
        self.stats_receiver.cleanup()
        self.client_handler.stop()
        self.client_handler.join()
        self.context.term()


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    process_loop(lambda: Runner())

    logging.info("Exiting gracefully")


main()
