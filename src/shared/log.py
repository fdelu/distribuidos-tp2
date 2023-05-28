import logging

DEFAULT_LOG_LEVEL = "INFO"
VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

FORMAT = "{asctime} | {levelname} | {funcName} | {message}"


def setup_logs(log_level: str | None = None) -> None:
    if log_level is not None:
        log_level = log_level.upper()
    invalid = False
    if log_level not in VALID_LOG_LEVELS:
        log_level = DEFAULT_LOG_LEVEL
        invalid = True

    logging.basicConfig(level=log_level, format=FORMAT, style="{")
    if invalid:
        logging.warn(
            f"Invalid or missing LOG_LEVEL supplied: '{log_level}'. "
            f"Defaulting to '{log_level}'"
        )
    logging.getLogger("pika").setLevel(logging.WARN)
