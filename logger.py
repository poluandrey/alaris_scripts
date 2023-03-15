import logging
import os
import sys

from dotenv import load_dotenv


load_dotenv()

LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
    }


def create_logger(logger_name, filename):
    log_dir = os.getenv('LOG_DIR')
    log_file = os.path.join(log_dir, filename)
    env_log_level = os.getenv('LOG_LEVEL')
    if env_log_level not in LOG_LEVELS.keys():
        print(f'unexpected log level. Please provide value from {LOG_LEVELS.keys()}')
        sys.exit()

    log_level = LOG_LEVELS[env_log_level]
    logger = logging.getLogger(logger_name)
    file_handler = logging.FileHandler(filename=log_file)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    return logger
