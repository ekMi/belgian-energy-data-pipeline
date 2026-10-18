"""Small CLI boundary: stderr logs and stable exit codes, no raw exceptions."""

import logging
import time


def configure_logging():
    formatter = logging.Formatter(
        "%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    formatter.converter = time.gmtime
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


def run_cli(main):
    configure_logging()
    logger = logging.getLogger("pipeline")
    try:
        main()
    except KeyboardInterrupt:
        logger.error("command_interrupted exit_code=130")
        return 130
    except Exception as error:
        # PostgreSQL exception details can contain source values or connection
        # information. Do not emit raw messages, DSNs, tracebacks or locals.
        logger.error("command_failed error_type=%s exit_code=1", type(error).__name__)
        return 1
    return 0
