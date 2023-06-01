import logging
from shared.log import setup_logs

from common.messages.joined import JoinedYearTrip
from common.messages.aggregated import PartialYearCounts

from ..common.aggregation_handler import AggregationHandler
from .aggregator import YearAggregator
from .comms import SystemCommunication
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = AggregationHandler[JoinedYearTrip, PartialYearCounts](
        comms, lambda: YearAggregator(config), config
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
