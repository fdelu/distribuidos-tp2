import logging
import zmq

from shared.log import setup_logs

from .handler import ClientHandler
from .config import Config


def main():
    config = Config()
    setup_logs(config.log_level)

    context = zmq.Context()
    context.setsockopt(zmq.LINGER, 0)  # Don't block on close

    handler = ClientHandler(config, context)
    handler.run()
    context.term()

    logging.info("Exiting gracefully")


main()
