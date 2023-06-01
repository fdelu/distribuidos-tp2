import logging
from shared.log import setup_logs

from common.messages.joined import JoinedRainTrip
from common.messages.aggregated import PartialRainAverages

from ..common.aggregation_handler import AggregationHandler
from ..common.config import Config
from .aggregator import RainAggregator
from .comms import SystemCommunication

NAME = "rain"


def main() -> None:
    config = Config(NAME)
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    handler = AggregationHandler[JoinedRainTrip, PartialRainAverages](
        comms, lambda: RainAggregator(), config
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
