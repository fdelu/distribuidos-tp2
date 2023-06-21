import logging

from shared.log import setup_logs

from .input_server import InputServer
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    server = InputServer(config)
    server.run()

    logging.info("Exiting gracefully")


main()
