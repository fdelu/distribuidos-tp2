import logging
from shared.log import setup_logs

from common.messages.joined import JoinedCityTrip
from common.messages.aggregated import PartialCityAverages

from ..common.aggregation_handler import AggregationHandler
from ..common.comms import AggregatorComms
from ..common.config import Config
from .aggregator import CityAggregator

NAME = "city"


def main() -> None:
    config = Config(NAME)
    setup_logs(config.log_level)

    comms = AggregatorComms[JoinedCityTrip, PartialCityAverages](config)
    handler = AggregationHandler[JoinedCityTrip, PartialCityAverages](
        comms, lambda: CityAggregator(), config
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
