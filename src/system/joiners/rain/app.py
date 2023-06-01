import logging
from shared.log import setup_logs

from common.messages.joined import JoinedRainTrip

from .config import Config
from .comms import SystemCommunication
from ..common.join_handler import JoinHandler
from .joiner import RainJoiner


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = JoinHandler[JoinedRainTrip](config, comms, lambda: RainJoiner(config))
    handler.run()
    logging.info("Exiting gracefully")


main()
