import logging
from shared.log import setup_logs

from common.messages.aggregated import PartialCityAverages
from common.util import process_loop

from ..common.reduction_handler import ReductionHandler
from ..common.comms import ReducerComms
from .config import Config
from .reducer import CityReducer


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: ReductionHandler[PartialCityAverages](
            ReducerComms[PartialCityAverages](Config()),
            Config(),
            lambda: CityReducer(),
        )
    )
    logging.info("Exiting gracefully")


main()
