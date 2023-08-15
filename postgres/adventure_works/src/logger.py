"""
Internal Module that handles Logging
"""
import logging
import sys

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = "script.log"


def get_console_handler() -> logging.StreamHandler:
    """
    Function to manage the console handler
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler() -> logging.FileHandler:
    """
    Function to get the handler
    """
    file_handler = logging.FileHandler(LOG_FILE, mode="a")
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name: str) -> logging.Logger:
    """
    Function to get the logger
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    logger.propagate = False
    return logger
