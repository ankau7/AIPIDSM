#!/usr/bin/env python3
"""
main.py

Main entry point for the Discovery Service of the AI-Powered Identity Risk Analytics Platform.
This service performs the following steps:
  1. Loads configuration settings from a YAML file.
  2. Sets up logging.
  3. Instantiates a Kafka producer to publish discovery events.
  4. Dynamically loads identity connectors from the connectors folder (for traditional sources).
  5. Uses the SaaS connector orchestrator to load SaaS identity connectors.
  6. Aggregates all identity data and publishes each record to a designated Kafka topic.

All configuration parameters (e.g., API endpoints, Kafka topics, credentials) are read from the YAML config,
which may be overridden by environment variables.

Author: [Your Name]
Date: [Current Date]
"""

import os
import sys
import yaml
import logging
import pathlib
import importlib.util

# Import Kafka producer module (assumed to be implemented)
from kafka_producer import KafkaProducerWrapper

# Import SaaS connector orchestrator
from connectors.saas_connectors.saas_connector_orchestrator import load_saas_connectors

def load_config(config_path: str) -> dict:
    """
    Loads configuration from a YAML file.

    Parameters:
        config_path (str): The path to the YAML configuration file.

    Returns:
        dict: The loaded configuration as a dictionary.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def setup_logging() -> None:
    """
    Sets up basic logging configuration.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def load_identity_connectors(config: dict) -> list:
    """
    Dynamically loads all identity connectors from the connectors directory.
    It scans the directory for Python files (excluding __init__.py and subdirectories like 'saas_connectors').
    For each connector module, it retrieves its configuration based on the filename and calls its fetch_identities() function.

    Parameters:
        config (dict): The global configuration dictionary.

    Returns:
        list: A list of identity records collected from all dynamically loaded connectors.
    """
    logger = logging.getLogger("DiscoveryService")
    identity_data = []
    # Determine the connectors directory (assumes main.py is in services/discovery-service/src/)
    connectors_dir = pathlib.Path(__file__).parent / "connectors"
    
    for file_path in connectors_dir.glob("*.py"):
        if file_path.name.startswith("__"):
            continue  # Skip __init__.py and any hidden files
        module_name = file_path.stem  # e.g., "ad_connector", "aws_connector"
        # Dynamically import the module
        spec = importlib.util.spec_from_file_location(module_name, str(file_path))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        # Check if the module has a fetch_identities() function
        if hasattr(module, "fetch_identities") and callable(module.fetch_identities):
            # Derive configuration key from module name by removing '_connector'
            config_key = module_name.replace("_connector", "")
            module_config = config.get(config_key, {})
            logger.info(f"Loading identities from connector: {module_name} using config key '{config_key}'")
            try:
                records = module.fetch_identities(module_config)
                identity_data.extend(records)
                logger.info(f"Fetched {len(records)} records from {module_name}")
            except Exception as e:
                logger.error(f"Error in connector {module_name}: {e}")
    return identity_data

def main():
    # Set up logging
    setup_logging()
    logger = logging.getLogger("DiscoveryService")
    logger.info("Starting Discovery Service...")

    # Load global configuration from YAML
    config_path = os.getenv("DISCOVERY_CONFIG_PATH", "services/discovery-service/config/discovery_config.yml")
    try:
        config = load_config(config_path)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    logger.info(f"Configuration loaded from {config_path}")

    # Initialize Kafka producer using settings from the config
    try:
        kafka_config = config.get("kafka", {})
        producer = KafkaProducerWrapper(kafka_config)
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        sys.exit(1)
    logger.info("Kafka producer initialized successfully.")

    # Dynamically load identity connectors (traditional sources)
    identity_data = load_identity_connectors(config)
    
    # Load SaaS connectors via orchestrator
    try:
        saas_config = config.get("saas", {})
        saas_connectors = load_saas_connectors(saas_config)
        saas_identity_data = []
        for connector in saas_connectors:
            try:
                data = connector.fetch_identities()
                saas_identity_data.extend(data)
            except Exception as e:
                logger.error(f"Error in SaaS connector {connector.__class__.__name__}: {e}")
        logger.info(f"Fetched {len(saas_identity_data)} identity records from SaaS applications.")
        identity_data.extend(saas_identity_data)
    except Exception as e:
        logger.error(f"Error fetching SaaS identity data: {e}")

    # Publish all collected identity records to Kafka
    identity_topic = kafka_config.get("identity_topic", "discovery-identity")
    logger.info(f"Publishing {len(identity_data)} identity records to Kafka topic '{identity_topic}'")
    for record in identity_data:
        try:
            producer.send(identity_topic, record)
        except Exception as e:
            logger.error(f"Error publishing record to Kafka: {e}")
    try:
        producer.flush()
    except Exception as e:
        logger.error(f"Error flushing Kafka producer: {e}")
    logger.info("All identity records published to Kafka successfully.")

if __name__ == "__main__":
    main()
