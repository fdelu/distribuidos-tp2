import logging
from shared.log import setup_logs

from common.messages.aggregated import PartialYearCounts
from common.util import process_loop

from ..common.reduction_handler import ReductionHandler
from ..common.comms import ReducerComms
from .config import Config
from .reducer import YearReducer


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: ReductionHandler[PartialYearCounts](
            ReducerComms[PartialYearCounts](Config()), Config(), lambda: YearReducer()
        )
    )
    logging.info("Exiting gracefully")


main()
