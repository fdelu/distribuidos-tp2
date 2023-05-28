import logging
from shared.log import setup_logs

from common.messages.joined import JoinedCityTrip
from common.messages.aggregated import PartialCityAverages

from ..common.aggregator import AggregationHandler
from ..common.config import Config
from .aggregator import CityAggregator
from .comms import SystemCommunication

NAME = "city"


def main():
    config = Config(NAME)
    setup_logs(config.log_level)

    comms = SystemCommunication(config)
    aggregator = CityAggregator()
    handler = AggregationHandler[JoinedCityTrip, PartialCityAverages](
        comms, aggregator, config
    )
    handler.run()
    logging.info("Exiting gracefully")


main()
