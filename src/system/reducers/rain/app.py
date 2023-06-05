import logging
from shared.log import setup_logs

from common.messages.aggregated import PartialRainAverages

from ..common.reduction_handler import ReductionHandler
from ..common.config import Config
from ..common.comms import ReducerComms
from .rain import RainReducer

NAME = "rain"


def main() -> None:
    config = Config(NAME)
    setup_logs(config.log_level)

    comms = ReducerComms[PartialRainAverages](config)
    handler = ReductionHandler[PartialRainAverages](
        comms, config, lambda: RainReducer()
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
