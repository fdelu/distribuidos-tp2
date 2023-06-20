import logging
from shared.log import setup_logs
from .bully import Bully
from .config import Config


def main() -> None:
    # TODO: sigterm handler
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    bully = Bully(config)
    bully.run()
    logging.info("finished medic")


main()
