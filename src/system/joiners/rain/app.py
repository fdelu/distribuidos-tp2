import logging
from shared.log import setup_logs

from common.messages.joined import JoinedRainTrip

from .config import Config
from .comms import SystemCommunication
from ..common.joiner import JoinHandler
from .joiner import RainJoiner


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    joiner = RainJoiner(config, comms)
    handler = JoinHandler[JoinedRainTrip](config, comms, joiner)
    handler.run()
    logging.info("Exiting gracefully")


main()
