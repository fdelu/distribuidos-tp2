import logging
from shared.log import setup_logs
from .config import Config
from ..common.comms_base.util import set_healthy


def main() -> None:
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    set_healthy("HEALTHY")


main()
