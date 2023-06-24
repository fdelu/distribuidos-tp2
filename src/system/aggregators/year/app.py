import logging
from shared.log import setup_logs

from common.messages.joined import JoinedYearTrip
from common.messages.aggregated import PartialYearCounts
from common.util import process_loop

from ..common.aggregation_handler import AggregationHandler
from ..common.comms import AggregatorComms
from .aggregator import YearAggregator
from .config import Config


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: AggregationHandler[JoinedYearTrip, PartialYearCounts](
            AggregatorComms[JoinedYearTrip, PartialYearCounts](Config()),
            lambda: YearAggregator(),
            Config(),
        )
    )
    logging.info("Exiting gracefully")


main()
