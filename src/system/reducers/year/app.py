import logging
from shared.log import setup_logs

from ..common.reduction_handler import ReductionHandler
from common.messages.aggregated import PartialYearCounts
from .year import YearReducer

from .config import Config
from .comms import SystemCommunication


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = ReductionHandler[PartialYearCounts](
        comms, config, lambda: YearReducer(config)
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
