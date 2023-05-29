import logging
from shared.log import setup_logs

from common.messages.joined import JoinedCityTrip

from .config import Config
from .comms import SystemCommunication
from ..common.joiner import JoinHandler
from .joiner import CityJoiner


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    joiner = CityJoiner(config, comms)
    handler = JoinHandler[JoinedCityTrip](config, comms, joiner)
    handler.run()
    logging.info("Exiting gracefully")


main()
