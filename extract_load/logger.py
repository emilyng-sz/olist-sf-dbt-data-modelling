import sys
import logging
import loguru
from loguru import logger

def setup_logger(log_file: str = "logs/pipeline.log") -> loguru._logger.Logger:

    # Remove default loguru sink
    logger.remove()

    # ── Console sink — human readable, coloured ───────────────────────────────
    logger.add(
        sys.stdout,
        level="DEBUG",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level:<8}</level> | "
            "<cyan>{name}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True
    )

    # ── File sink — structured JSON, rotation, retention ──────────────────────
    logger.add(
        log_file,
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name} | {message}",
        rotation="50 MB",       # new file after 50MB
        # retention="30 days",    # auto-delete logs older than 30 days
        compression="zip",      # compress rotated files
        encoding="utf-8",
        enqueue=True            # async-safe writes
    )

    # ── Intercept standard logging (captures dbt, snowflake connector logs) ───
    class InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            frame, depth = sys._getframe(6), 6
            while frame and frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    # ── Whitelist only the loggers you care about ─────────────────────────────
    # Add "dbt" here when you initialise your dbt project
    WATCHED_LOGGERS = [
        "main",         # your ingestion script
        "ingestion_raw",
        "table_schemas", # your schema module
        "dbt"
    ]

    for name in WATCHED_LOGGERS:
        watched = logging.getLogger(name)
        watched.handlers = [InterceptHandler()]
        watched.propagate = False

    # ── Silence noisy third-party libraries explicitly ────────────────────────
    SILENCED_LOGGERS = [
        "boto3",
        "botocore",
        "urllib3",
        "snowflake.connector",
        "snowflake.connector.network",
        "snowflake.connector.cursor",
        "s3transfer",
    ]

    for name in SILENCED_LOGGERS:
        logging.getLogger(name).setLevel(logging.CRITICAL)
    return logger