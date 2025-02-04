#!/usr/bin/env python3
"""
log_formatter.py

This module defines a custom log formatter for the AI-Powered Identity Risk Analytics Platform.
It extends Python's built-in logging.Formatter to provide structured, contextual log output including:
    - Timestamp
    - Hostname
    - Process ID
    - Thread name
    - Logger name
    - Log level
    - Message

This formatter is designed for compatibility with log aggregators and monitoring systems.
All configuration values (e.g., format strings and date format) are defined within the class
and can be overridden by passing parameters during initialization.

Author: [Your Name]
Date: [Current Date]
"""

import logging
import socket


class CustomLogFormatter(logging.Formatter):
    """
    CustomLogFormatter extends logging.Formatter to include hostname and other context in log messages.
    
    Default Format:
        %(asctime)s - %(hostname)s - Process:%(process)d - Thread:%(threadName)s - %(name)s - %(levelname)s - %(message)s

    Default Date Format:
        %Y-%m-%d %H:%M:%S
    """

    def __init__(self, fmt=None, datefmt=None, style='%', extra_fields=None):
        """
        Initialize the custom log formatter.

        Parameters:
            fmt (str): The log message format string. If None, a default format is used.
            datefmt (str): The date format string. If None, defaults to "%Y-%m-%d %H:%M:%S".
            style (str): The format style (default is '%').
            extra_fields (dict): Optional dictionary of extra fields to include in every log record.
        """
        if fmt is None:
            fmt = "%(asctime)s - %(hostname)s - Process:%(process)d - Thread:%(threadName)s - %(name)s - %(levelname)s - %(message)s"
        if datefmt is None:
            datefmt = "%Y-%m-%d %H:%M:%S"
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.extra_fields = extra_fields or {}
        # Obtain the current hostname once; this value will be attached to every record.
        self.hostname = socket.gethostname()

    def format(self, record):
        """
        Format the specified record as text.

        This method injects the hostname and any extra fields into the record before formatting.
        """
        # Add the hostname to the record
        record.hostname = self.hostname
        # Add any extra fields to the record
        for key, value in self.extra_fields.items():
            setattr(record, key, value)
        # Call the parent class's format method
        return super().format(record)


if __name__ == "__main__":
    # Demonstration of the CustomLogFormatter usage:
    # Create a logger instance with a specific name.
    logger = logging.getLogger("CustomLogger")
    logger.setLevel(logging.DEBUG)

    # Create a console handler that outputs to stdout.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Initialize the custom log formatter with default values.
    formatter = CustomLogFormatter()
    console_handler.setFormatter(formatter)

    # Add the console handler to the logger.
    logger.addHandler(console_handler)

    # Log messages at various levels to demonstrate output formatting.
    logger.debug("Debug: This is a debug message.")
    logger.info("Info: This is an info message.")
    logger.warning("Warning: This is a warning message.")
    logger.error("Error: This is an error message.")
    logger.critical("Critical: This is a critical message.")
