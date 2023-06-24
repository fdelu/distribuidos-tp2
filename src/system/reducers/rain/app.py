import logging
from shared.log import setup_logs

from common.messages.aggregated import PartialRainAverages
from common.util import process_loop

from ..common.reduction_handler import ReductionHandler
from ..common.comms import ReducerComms
from .config import Config
from .reducer import RainReducer


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: ReductionHandler[PartialRainAverages](
            ReducerComms[PartialRainAverages](Config()), Config(), lambda: RainReducer()
        )
    )
    logging.info("Exiting gracefully")


main()
