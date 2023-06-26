import logging
from shared.log import setup_logs
from .config import Config
from .bully import Bully


def main() -> None:
    setup_logs(Config().log_level)
    logging.info("started medic")
    bully = Bully()
    bully.run()
    logging.info("finished medic")


main()
