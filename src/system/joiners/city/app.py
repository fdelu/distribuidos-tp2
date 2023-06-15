import logging
from shared.log import setup_logs

from common.messages.joined import JoinedCityTrip

from ..common.comms import JoinerComms
from ..common.join_handler import JoinHandler
from .joiner import CityJoiner
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = JoinerComms[JoinedCityTrip](config)
    handler = JoinHandler[JoinedCityTrip](config, comms, lambda: CityJoiner(config))
    handler.run()
    logging.info("Exiting gracefully")


main()
