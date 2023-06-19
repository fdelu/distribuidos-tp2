import logging
from shared.log import setup_logs

from common.messages.joined import JoinedRainTrip

from ..common.comms import JoinerComms
from ..common.join_handler import JoinHandler
from .joiner import RainJoiner
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = JoinerComms[JoinedRainTrip](config)
    handler = JoinHandler[JoinedRainTrip](config, comms, lambda: RainJoiner(config))
    handler.run()
    logging.info("Exiting gracefully")


main()
