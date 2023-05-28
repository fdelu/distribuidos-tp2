import logging
from shared.log import setup_logs

from .config import Config
from .comms import SystemCommunication
from ..common.joiner import JoinHandler
from .joiner import YearJoiner


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    joiner = YearJoiner(config, comms)
    handler = JoinHandler(config, comms, joiner)
    handler.run()
    logging.info("Exiting gracefully")


main()
