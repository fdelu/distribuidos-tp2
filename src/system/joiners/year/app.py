import logging
from shared.log import setup_logs

from common.messages.joined import JoinedYearTrip
from common.util import process_loop

from ..common.comms import JoinerComms
from ..common.join_handler import JoinHandler
from .joiner import YearJoiner
from .config import Config


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: JoinHandler[JoinedYearTrip](
            Config(), JoinerComms[JoinedYearTrip](Config()), lambda: YearJoiner()
        )
    )
    logging.info("Exiting gracefully")


main()
