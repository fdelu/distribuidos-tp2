import logging
from shared.log import setup_logs
from common.comms_base.util import set_healthy
from .bully import Bully
from .config import Config


def main() -> None:
    # TODO: sigterm handler
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    set_healthy("HEALTHY")
    bully = Bully(config)
    bully.run()
    logging.info("finished medic")


main()
