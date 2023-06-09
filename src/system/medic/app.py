import logging
from shared.log import setup_logs
from common.comms_base.util import set_healthy

from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    set_healthy("HEALTHY")


main()
