import logging
from shared.log import setup_logs

from common.messages.aggregated import PartialYearCounts

from ..common.reduction_handler import ReductionHandler
from ..common.comms import ReducerComms
from .year import YearReducer
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = ReducerComms[PartialYearCounts](config)
    handler = ReductionHandler[PartialYearCounts](
        comms, config, lambda: YearReducer(config)
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
