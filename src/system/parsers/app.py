import logging

from shared.log import setup_logs

from .config import Config
from .record_parser import RecordParser

trips = False


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    parser = RecordParser(config)
    parser.run()
    logging.info("Exiting gracefully")


main()
