import logging
from shared.log import setup_logs

from common.messages.joined import JoinedRainTrip
from common.messages.aggregated import PartialRainAverages
from common.util import process_loop

from ..common.aggregation_handler import AggregationHandler
from ..common.comms import AggregatorComms
from .config import Config
from .aggregator import RainAggregator


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: AggregationHandler[JoinedRainTrip, PartialRainAverages](
            AggregatorComms[JoinedRainTrip, PartialRainAverages](Config()),
            lambda: RainAggregator(),
            Config(),
        )
    )
    logging.info("Exiting gracefully")


main()
