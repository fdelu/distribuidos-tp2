import logging
from shared.log import setup_logs

from common.messages.joined import JoinedCityTrip
from common.messages.aggregated import PartialCityAverages
from common.util import process_loop

from ..common.aggregation_handler import AggregationHandler
from ..common.comms import AggregatorComms
from .config import Config
from .aggregator import CityAggregator


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: AggregationHandler[JoinedCityTrip, PartialCityAverages](
            AggregatorComms[JoinedCityTrip, PartialCityAverages](Config()),
            lambda: CityAggregator(),
            Config(),
        )
    )
    logging.info("Exiting gracefully")


main()
