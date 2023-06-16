import logging

from shared.log import setup_logs

from .config import Config
from .parse_handler import ParseHandler
from .comms import SystemCommunication

trips = False


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    parser = ParseHandler(comms)
    parser.run()
    logging.info("Exiting gracefully")


main()
