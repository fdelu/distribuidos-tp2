import logging
from shared.log import setup_logs

from common.messages.joined import JoinedCityTrip

from .config import Config
from .comms import SystemCommunication
from ..common.join_handler import JoinHandler
from .joiner import CityJoiner


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = JoinHandler[JoinedCityTrip](config, comms, lambda: CityJoiner(config))
    handler.run()
    logging.info("Exiting gracefully")


main()
