import logging
from shared.log import setup_logs

from ..common.reducer import ReductionHandler
from common.messages.aggregated import PartialYearCounts
from .year import YearReducer

from .config import Config
from .comms import SystemCommunication


def main():
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    reducer = YearReducer(config)
    handler = ReductionHandler[PartialYearCounts](config, reducer, comms)
    handler.run()
    logging.info("Exiting gracefully")


main()
