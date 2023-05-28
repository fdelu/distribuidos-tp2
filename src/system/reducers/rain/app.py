import logging
from shared.log import setup_logs

from ..common.reducer import ReductionHandler
from common.messages.aggregated import PartialRainAverages
from .rain import RainReducer
from ..common.config import Config

from .comms import SystemCommunication

NAME = "rain"


def main() -> None:
    config = Config(NAME)
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    reducer = RainReducer()
    handler = ReductionHandler[PartialRainAverages](config, reducer, comms)
    handler.run()
    logging.info("Exiting gracefully")


main()
