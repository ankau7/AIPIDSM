#!/usr/bin/env python3
"""
helpers.py

This module provides miscellaneous helper functions for the AI-Powered Identity Risk Analytics Platform.
Functions include utilities for generating random IDs, safely retrieving values from dictionaries,
formatting timestamps, merging dictionaries, pretty-printing dictionaries, and checking if a value is numeric.

Author: [Your Name]
Date: [Current Date]
"""

import random
import string
import datetime
import json
from typing import Any, Dict, Optional

def generate_random_id(length: int = 8) -> str:
    """
    Generates a random string of uppercase letters and digits.

    Parameters:
        length (int): The length of the random ID to generate (default is 8).

    Returns:
        str: A randomly generated string.
    """
    characters = string.ascii_uppercase + string.digits
    random_id = ''.join(random.choice(characters) for _ in range(length))
    return random_id

def safe_get(dictionary: Dict[Any, Any], key: Any, default: Optional[Any] = None) -> Any:
    """
    Safely retrieves a value from a dictionary. If the key does not exist, returns a default value.

    Parameters:
        dictionary (Dict[Any, Any]): The dictionary to retrieve the value from.
        key (Any): The key to look up.
        default (Optional[Any]): The default value to return if key is not found (default is None).

    Returns:
        Any: The value associated with the key, or the default if the key is not present.
    """
    return dictionary.get(key, default)

def format_timestamp(timestamp: Optional[datetime.datetime] = None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Formats a given timestamp into a string using the specified format.
    If no timestamp is provided, uses the current UTC time.

    Parameters:
        timestamp (Optional[datetime.datetime]): The datetime object to format. If None, uses current UTC time.
        fmt (str): The format string (default is "%Y-%m-%d %H:%M:%S").

    Returns:
        str: The formatted timestamp.
    """
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()
    return timestamp.strftime(fmt)

def merge_dicts(dict1: Dict[Any, Any], dict2: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Merges two dictionaries. Values from the second dictionary override those from the first.
    This is a shallow merge; nested dictionaries are not deeply merged.

    Parameters:
        dict1 (Dict[Any, Any]): The first dictionary.
        dict2 (Dict[Any, Any]): The second dictionary, whose values override dict1.

    Returns:
        Dict[Any, Any]: The merged dictionary.
    """
    merged = dict1.copy()
    merged.update(dict2)
    return merged

def pretty_print_dict(data: Dict[Any, Any]) -> None:
    """
    Prints a dictionary in a human-readable JSON format.

    Parameters:
        data (Dict[Any, Any]): The dictionary to print.
    """
    print(json.dumps(data, indent=4, sort_keys=True))

def is_numeric(value: Any) -> bool:
    """
    Checks whether a value can be converted to a float.

    Parameters:
        value (Any): The value to check.

    Returns:
        bool: True if the value is numeric, False otherwise.
    """
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

if __name__ == "__main__":
    # Demonstration of helper functions:
    
    # Generate and display a random ID of length 12.
    random_id = generate_random_id(12)
    print("Random ID:", random_id)
    
    # Demonstrate safe_get functionality.
    sample_dict = {"name": "Alice", "age": 30}
    print("Safe Get (name):", safe_get(sample_dict, "name"))
    print("Safe Get (non-existent key):", safe_get(sample_dict, "address", "Not Provided"))
    
    # Format and display the current timestamp.
    formatted_time = format_timestamp()
    print("Formatted Timestamp:", formatted_time)
    
    # Merge two dictionaries and pretty-print the result.
    dict_a = {"key1": "value1", "key2": "value2"}
    dict_b = {"key2": "override_value", "key3": "value3"}
    merged_dict = merge_dicts(dict_a, dict_b)
    print("Merged Dictionary:")
    pretty_print_dict(merged_dict)
    
    # Check numeric values.
    print("Is '123.45' numeric?", is_numeric("123.45"))
    print("Is 'abc' numeric?", is_numeric("abc"))
