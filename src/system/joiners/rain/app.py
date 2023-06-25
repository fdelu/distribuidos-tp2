import logging
from shared.log import setup_logs

from common.messages.joined import JoinedRainTrip
from common.util import process_loop

from ..common.comms import JoinerComms
from ..common.join_handler import JoinHandler
from .joiner import RainJoiner
from .config import Config


def main() -> None:
    setup_logs(Config().log_level)

    process_loop(
        lambda: JoinHandler[JoinedRainTrip](
            Config(), JoinerComms[JoinedRainTrip](Config()), lambda: RainJoiner()
        )
    )
    logging.info("Exiting gracefully")


main()
