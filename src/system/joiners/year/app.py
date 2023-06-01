import logging
from shared.log import setup_logs

from common.messages.joined import JoinedYearTrip

from .config import Config
from .comms import SystemCommunication
from ..common.join_handler import JoinHandler
from .joiner import YearJoiner


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = JoinHandler[JoinedYearTrip](config, comms, lambda: YearJoiner(config))
    handler.run()
    logging.info("Exiting gracefully")


main()
