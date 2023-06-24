import logging

from shared.log import setup_logs
from common.util import process_loop

from .input_server import InputServer
from .config import Config


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(lambda: InputServer())

    logging.info("Exiting gracefully")


main()
