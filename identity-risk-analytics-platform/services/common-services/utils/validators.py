#!/usr/bin/env python3
"""
validators.py

This module provides a set of validation functions for the AI-Powered Identity Risk Analytics Platform.
These validators help ensure that data is properly formatted and meets expected criteria before processing.

Functions included:
    - is_valid_email(email: str) -> bool: Validates that the provided string is a properly formatted email address.
    - is_valid_url(url: str) -> bool: Validates that the provided string is a properly formatted URL.
    - validate_required_keys(data: dict, keys: list) -> bool: Checks that a dictionary contains all required keys.
    - validate_positive_number(value: Any) -> bool: Verifies that the value is a positive number (int or float).
    - validate_non_empty_string(value: Any) -> bool: Ensures that the value is a non-empty string.
    - validate_string_length(value: str, min_length: int, max_length: int) -> bool: Checks that the length of the string is within specified bounds.

Author: [Your Name]
Date: [Current Date]
"""

import re
from typing import Any, Dict, List

def is_valid_email(email: str) -> bool:
    """
    Validates if the provided string is a valid email address.

    Parameters:
        email (str): The email address to validate.

    Returns:
        bool: True if the email is valid, False otherwise.
    """
    email_regex = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    return bool(email_regex.match(email))

def is_valid_url(url: str) -> bool:
    """
    Validates if the provided string is a valid URL.

    Parameters:
        url (str): The URL to validate.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    url_regex = re.compile(
        r'^(?:http|ftp)s?://'                  # http:// or https://
        r'(?:\S+(?::\S*)?@)?'                  # optional user:pass@
        r'(?:(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,6}|'  # domain...
        r'localhost|'                          # localhost...
        r'\d{1,3}(?:\.\d{1,3}){3})'            # ...or IPv4
        r'(?::\d+)?'                          # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return bool(url_regex.match(url))

def validate_required_keys(data: Dict[Any, Any], keys: List[str]) -> bool:
    """
    Validates that the provided dictionary contains all required keys.

    Parameters:
        data (dict): The dictionary to check.
        keys (list of str): The list of keys that must be present in the dictionary.

    Returns:
        bool: True if all keys are present, False otherwise.
    """
    for key in keys:
        if key not in data:
            return False
    return True

def validate_positive_number(value: Any) -> bool:
    """
    Validates that the provided value is a positive number (int or float).

    Parameters:
        value (Any): The value to check.

    Returns:
        bool: True if the value is a positive number, False otherwise.
    """
    try:
        number = float(value)
        return number > 0
    except (ValueError, TypeError):
        return False

def validate_non_empty_string(value: Any) -> bool:
    """
    Validates that the provided value is a non-empty string.

    Parameters:
        value (Any): The value to check.

    Returns:
        bool: True if the value is a non-empty string, False otherwise.
    """
    return isinstance(value, str) and len(value.strip()) > 0

def validate_string_length(value: str, min_length: int = 1, max_length: int = 255) -> bool:
    """
    Validates that the length of a string is within the specified bounds.

    Parameters:
        value (str): The string to validate.
        min_length (int): The minimum allowed length (default is 1).
        max_length (int): The maximum allowed length (default is 255).

    Returns:
        bool: True if the string length is within bounds, False otherwise.
    """
    if not isinstance(value, str):
        return False
    length = len(value)
    return min_length <= length <= max_length

if __name__ == "__main__":
    # Demonstration of validators functionality:
    
    # Test email validation.
    test_email = "user@example.com"
    print(f"Is '{test_email}' a valid email? {is_valid_email(test_email)}")
    
    # Test URL validation.
    test_url = "https://www.example.com"
    print(f"Is '{test_url}' a valid URL? {is_valid_url(test_url)}")
    
    # Test required keys validation.
    sample_dict = {"name": "Alice", "age": 30}
    required_keys = ["name", "age"]
    print(f"Does sample_dict contain {required_keys}? {validate_required_keys(sample_dict, required_keys)}")
    
    # Test positive number validation.
    test_number = "42"
    print(f"Is '{test_number}' a positive number? {validate_positive_number(test_number)}")
    
    # Test non-empty string validation.
    test_string = "  Hello World  "
    print(f"Is '{test_string}' a non-empty string? {validate_non_empty_string(test_string)}")
    
    # Test string length validation.
    test_str_length = "Test"
    print(f"Is '{test_str_length}' between 2 and 10 characters? {validate_string_length(test_str_length, 2, 10)}")
