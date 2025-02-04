#!/usr/bin/env python3
"""
logger.py

This module provides a centralized logging configuration for the AI-Powered Identity Risk Analytics Platform.
It sets up loggers that output to both the console and a rotating file handler.
All configuration values (log level, file path, etc.) are configurable via environment variables.

Author: [Your Name]
Date: [Current Date]
"""

import logging
import logging.handlers
import os
import sys

# Configuration: These values can be overridden via environment variables.
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")           # Default logging level is DEBUG
LOG_FILE = os.getenv("LOG_FILE", "app.log")             # Default log file name
LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG_DATE_FORMAT = os.getenv("LOG_DATE_FORMAT", "%Y-%m-%d %H:%M:%S")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", 10 * 1024 * 1024))  # 10 MB default size
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", 5))              # Default to 5 backup files

def get_logger(logger_name: str) -> logging.Logger:
    """
    Create and return a logger with the specified name.
    This logger is configured to log messages to both the console (stdout) and a rotating file.

    Parameters:
        logger_name (str): The name for the logger.

    Returns:
        logging.Logger: A configured logger instance.
    """
    # Retrieve the logger by name. If it already exists with handlers, return it.
    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():
        return logger

    # Set the log level from environment variable (defaults to DEBUG)
    level = getattr(logging, LOG_LEVEL.upper(), logging.DEBUG)
    logger.setLevel(level)

    # Create a formatter using the specified format and date format.
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Console handler: outputs logs to stdout.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # Rotating file handler: writes logs to a file and rotates them when they reach a specified size.
    file_handler = logging.handlers.RotatingFileHandler(
        filename=LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # Add both handlers to the logger.
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Prevent log messages from propagating to the root logger to avoid duplicate logging.
    logger.propagate = False

    return logger

if __name__ == "__main__":
    # When run as a script, this section tests the logger functionality.
    test_logger = get_logger("TestLogger")
    test_logger.debug("Debug: This is a debug message.")
    test_logger.info("Info: This is an info message.")
    test_logger.warning("Warning: This is a warning message.")
    test_logger.error("Error: This is an error message.")
    test_logger.critical("Critical: This is a critical message.")
