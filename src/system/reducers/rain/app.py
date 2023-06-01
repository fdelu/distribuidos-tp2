import logging
from shared.log import setup_logs

from ..common.reduction_handler import ReductionHandler
from common.messages.aggregated import PartialRainAverages
from .rain import RainReducer
from ..common.config import Config

from .comms import SystemCommunication

NAME = "rain"


def main() -> None:
    config = Config(NAME)
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = ReductionHandler[PartialRainAverages](
        comms, config, lambda: RainReducer()
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
