import logging
import zmq

from shared.log import setup_logs

from .stats import Stats
from .stats.receiver import StatsReceiver
from .handler import ClientHandler
from .config import Config


def main():
    config = Config()
    setup_logs(config.log_level)
    context = zmq.Context()
    context.setsockopt(zmq.LINGER, 0)  # Don't block on close

    stats = Stats()
    stat_receiver = StatsReceiver(config, stats)
    client_handler = ClientHandler(config, context, stats)
    stat_receiver.add_listener(client_handler)

    client_handler.start()
    stat_receiver.run()

    client_handler.stop()
    client_handler.join()
    context.term()

    logging.info("Exiting gracefully")


main()
