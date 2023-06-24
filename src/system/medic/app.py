import logging
from shared.log import setup_logs
from .config import Config
from .bully import Bully


def main() -> None:
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    bully = Bully(config)
    bully.run()
    logging.info("finished medic")


main()
