import logging
from shared.log import setup_logs

from common.messages.joined import JoinedYearTrip

from ..common.comms import JoinerComms
from ..common.join_handler import JoinHandler
from .joiner import YearJoiner
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = JoinerComms[JoinedYearTrip](config)
    handler = JoinHandler[JoinedYearTrip](config, comms, lambda: YearJoiner(config))
    handler.run()
    logging.info("Exiting gracefully")


main()
