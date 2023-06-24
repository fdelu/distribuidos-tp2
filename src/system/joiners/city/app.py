import logging
from shared.log import setup_logs

from common.messages.joined import JoinedCityTrip
from common.util import process_loop

from ..common.comms import JoinerComms
from ..common.join_handler import JoinHandler
from .joiner import CityJoiner
from .config import Config


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: JoinHandler[JoinedCityTrip](
            Config(), JoinerComms[JoinedCityTrip](Config()), lambda: CityJoiner()
        )
    )
    logging.info("Exiting gracefully")


main()
