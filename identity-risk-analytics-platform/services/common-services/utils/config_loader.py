#!/usr/bin/env python3
"""
config_loader.py

This module provides utility functions for loading and merging global configuration for the
AI-Powered Identity Risk Analytics Platform. It loads configuration values from a YAML file
and allows environment variables to override specific keys in the configuration.

Usage:
    import config_loader
    config = config_loader.load_config()  # Loads from default path or environment variable CONFIG_PATH

Author: [Your Name]
Date: [Current Date]
"""

import os
import yaml


def load_yaml_config(file_path: str) -> dict:
    """
    Loads configuration from a YAML file.

    Parameters:
        file_path (str): The path to the YAML configuration file.

    Returns:
        dict: A dictionary containing the configuration data.
    
    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, "r") as file:
        try:
            config = yaml.safe_load(file)
            if config is None:
                config = {}
            return config
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML configuration file: {e}")


def merge_env_config(config: dict) -> dict:
    """
    Overrides configuration values from the loaded config with environment variables if they exist.
    For each key in the config that has a corresponding environment variable (in uppercase), the value
    will be overridden. This is done recursively for nested dictionaries.

    Parameters:
        config (dict): The configuration dictionary loaded from YAML.

    Returns:
        dict: The updated configuration dictionary with environment variable values overriding YAML.
    """
    for key, value in config.items():
        env_var = key.upper()
        if isinstance(value, dict):
            # Recursively merge nested dictionaries
            config[key] = merge_env_config(value)
        elif env_var in os.environ:
            config[key] = os.environ[env_var]
    return config


def load_config(file_path: str = None) -> dict:
    """
    Loads the global configuration by first determining the configuration file path.
    It then loads the YAML configuration and merges it with environment variables.

    Parameters:
        file_path (str): Optional path to the configuration file. If not provided, the function
                         checks the environment variable CONFIG_PATH; if not set, it defaults to
                         'config/config.yml'.

    Returns:
        dict: The final configuration dictionary.
    """
    if file_path is None:
        file_path = os.getenv("CONFIG_PATH", "config/config.yml")
    config = load_yaml_config(file_path)
    config = merge_env_config(config)
    return config


def get_config_value(config: dict, key: str, default=None):
    """
    Retrieves a configuration value for a given key from the configuration dictionary.

    Parameters:
        config (dict): The configuration dictionary.
        key (str): The key for which to retrieve the value.
        default: The default value to return if the key is not found.

    Returns:
        The configuration value if found, otherwise the default value.
    """
    return config.get(key, default)


if __name__ == "__main__":
    try:
        # Example usage: load configuration from the default path or CONFIG_PATH environment variable.
        config = load_config()
        print("Configuration loaded successfully:")
        print(config)
    except Exception as e:
        print("Error loading configuration:", e)
