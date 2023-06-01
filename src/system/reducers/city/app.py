import logging
from shared.log import setup_logs

from ..common.reduction_handler import ReductionHandler
from common.messages.aggregated import PartialCityAverages
from .city import CityReducer

from .config import Config
from .comms import SystemCommunication


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = ReductionHandler[PartialCityAverages](
        comms, config, lambda: CityReducer(config)
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
