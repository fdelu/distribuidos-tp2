import logging
from shared.log import setup_logs

from common.messages.aggregated import PartialCityAverages

from ..common.reduction_handler import ReductionHandler
from ..common.comms import ReducerComms
from .city import CityReducer
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = ReducerComms[PartialCityAverages](config)
    handler = ReductionHandler[PartialCityAverages](
        comms, config, lambda: CityReducer(config)
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
