import logging

from shared.log import setup_logs
from common.util import process_loop

from .config import Config
from .parse_handler import ParseHandler
from .comms import SystemCommunication


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(lambda: ParseHandler(SystemCommunication()))
    logging.info("Exiting gracefully")


main()
