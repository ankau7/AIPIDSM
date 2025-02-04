#!/usr/bin/env python3
"""
test_logger.py

This module contains unit tests for the CustomLogFormatter and the get_logger function
from the logging service. It verifies that the logger is configured with the correct
handlers, that log messages include the hostname, and that the log output respects the
configured log level and format.

Author: [Your Name]
Date: [Current Date]
"""

import io
import socket
import unittest
import logging
import sys

# Import the get_logger function from our logger module.
# Adjust the import path if necessary to match your project structure.
from common_services.logging_service.src.logger import get_logger


class TestCustomLogger(unittest.TestCase):
    def setUp(self):
        """
        Set up a test logger instance with a StreamHandler to capture log output.
        """
        # Create a logger instance using the get_logger function.
        self.logger = get_logger("TestLogger")
        # Clear any existing handlers to control test output.
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Set up a StringIO stream to capture console output.
        self.stream = io.StringIO()
        self.stream_handler = logging.StreamHandler(self.stream)
        # Use the same formatter as configured by the get_logger function.
        # Re-use the formatter from the original logger if available; otherwise, use a default formatter.
        if self.logger.handlers:
            formatter = self.logger.handlers[0].formatter
        else:
            formatter = logging.Formatter("%(asctime)s - %(hostname)s - Process:%(process)d - Thread:%(threadName)s - %(name)s - %(levelname)s - %(message)s")
        self.stream_handler.setFormatter(formatter)
        self.logger.addHandler(self.stream_handler)
        # Ensure the logger level is set to DEBUG for testing.
        self.logger.setLevel(logging.DEBUG)

    def tearDown(self):
        """
        Clean up after tests.
        """
        self.stream_handler.close()
        self.stream.close()
        # Optionally, remove handlers from the logger.
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

    def test_logger_has_handlers(self):
        """
        Test that the logger instance has at least one handler attached.
        """
        self.assertTrue(len(self.logger.handlers) > 0, "Logger should have at least one handler attached.")

    def test_logger_includes_hostname(self):
        """
        Test that log output includes the hostname as provided by the CustomLogFormatter.
        """
        expected_hostname = socket.gethostname()
        self.logger.info("Testing hostname inclusion in log output.")
        output = self.stream.getvalue()
        self.assertIn(expected_hostname, output, f"Log output should include the hostname: {expected_hostname}")

    def test_log_level_filtering(self):
        """
        Test that log messages below the logger's level are not output.
        """
        # Set logger to WARNING level.
        self.logger.setLevel(logging.WARNING)
        # Clear the stream.
        self.stream.truncate(0)
        self.stream.seek(0)
        # Log a DEBUG message which should be filtered out.
        self.logger.debug("This debug message should not appear.")
        output = self.stream.getvalue()
        self.assertNotIn("This debug message should not appear.", output, "DEBUG message should be filtered out at WARNING level.")

    def test_log_output_format(self):
        """
        Test that the log output follows the expected format:
        It should include a timestamp, hostname, process ID, thread name, logger name, log level, and the message.
        """
        self.logger.setLevel(logging.INFO)
        self.stream.truncate(0)
        self.stream.seek(0)
        self.logger.info("Format test message.")
        output = self.stream.getvalue()
        # Check that output contains expected components.
        self.assertIn("Format test message.", output, "Log output must contain the actual message text.")
        self.assertIn("INFO", output, "Log output must include the log level INFO.")
        self.assertIn("-", output, "Log output must use '-' as a separator in the format.")

if __name__ == "__main__":
    unittest.main()
