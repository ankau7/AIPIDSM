#!/usr/bin/env python3
"""
saas_connector_orchestrator.py

This module provides an orchestrator for dynamically loading and instantiating SaaS connector plugins.
It reads a configuration dictionary (typically loaded from YAML or another configuration source)
that contains definitions for various SaaS integrations under a "saas" key.
For each SaaS connector entry that is enabled, the orchestrator performs the following:
    - Determines the expected module filename and class name based on a naming convention.
    - Loads the plugin module dynamically from a configurable plugin directory.
    - Validates the plugin configuration.
    - Instantiates the connector using its provided configuration.
    - Verifies that the instantiated connector implements the required interface.
All instantiated connector objects are returned as a list for use by the Discovery Service.

Author: [Your Name]
Date: [Current Date]
"""

import os
import logging
import time
import asyncio
import importlib.util
from typing import List, Dict, Any

logger = logging.getLogger("SaasConnectorOrchestrator")
logger.setLevel(logging.DEBUG)

def load_saas_connectors(config: Dict[str, Any]) -> List[Any]:
    """
    Dynamically loads and instantiates all enabled SaaS connector plugins based on the provided configuration.

    The configuration dictionary should have a "saas" key mapping plugin names to their settings.
    Each plugin's configuration should include an "enabled" key (defaulting to True if omitted)
    and a "config" key containing the plugin-specific configuration.

    The plugin directory is configurable via the environment variable SAAS_PLUGIN_DIR; if not set,
    it defaults to the "plugins/saas" directory relative to this file.

    Returns:
        List[Any]: A list of instantiated SaaS connector objects.
    """
    connectors = []
    start_time = time.time()

    saas_config = config.get("saas", {})
    if not saas_config:
        logger.warning("No SaaS connector configuration found under 'saas' key.")
        return connectors

    # Determine the plugins directory (configurable via SAAS_PLUGIN_DIR)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_plugins_dir = os.path.join(base_dir, "plugins", "saas")
    plugins_dir = os.getenv("SAAS_PLUGIN_DIR", default_plugins_dir)
    logger.debug(f"Using SaaS plugin directory: {plugins_dir}")

    async def load_plugin(plugin_name: str, plugin_settings: Dict[str, Any]) -> Any:
        """
        Coroutine to load and instantiate a single SaaS connector plugin.

        Parameters:
            plugin_name (str): The name of the plugin (e.g., "m365").
            plugin_settings (dict): The configuration settings for the plugin.

        Returns:
            The instantiated connector object, or None if loading fails.
        """
        enabled = plugin_settings.get("enabled", True)
        if not enabled:
            logger.info(f"SaaS connector '{plugin_name}' is disabled. Skipping.")
            return None

        # Validate that a "config" key exists in the plugin settings.
        if "config" not in plugin_settings:
            logger.error(f"SaaS connector '{plugin_name}' configuration missing 'config' key. Skipping.")
            return None

        module_filename = f"{plugin_name}_connector.py"
        module_path = os.path.join(plugins_dir, module_filename)
        if not os.path.exists(module_path):
            logger.error(f"Plugin file '{module_filename}' not found in '{plugins_dir}'. Skipping plugin '{plugin_name}'.")
            return None

        try:
            spec = importlib.util.spec_from_file_location(plugin_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            logger.error(f"Failed to load plugin module '{module_filename}' for '{plugin_name}': {e}")
            return None

        # Derive the expected class name, e.g. "m365" -> "M365Connector"
        class_name = plugin_name.capitalize() + "Connector"
        if not hasattr(module, class_name):
            logger.error(f"Plugin module '{module_filename}' does not contain expected class '{class_name}'. Skipping.")
            return None

        connector_class = getattr(module, class_name)
        try:
            instance = connector_class(plugin_settings.get("config", {}))
        except Exception as e:
            logger.error(f"Error instantiating SaaS connector '{class_name}' for plugin '{plugin_name}': {e}")
            return None

        # Verify that the instance implements the required interface.
        if not callable(getattr(instance, "fetch_identities", None)):
            logger.error(f"Connector '{class_name}' does not implement fetch_identities(). Skipping.")
            return None

        logger.info(f"Loaded SaaS connector: {class_name} for plugin '{plugin_name}'.")
        return instance

    # Asynchronously load all plugins concurrently.
    loop = asyncio.get_running_loop()
    tasks = []
    for plugin_name, plugin_settings in saas_config.items():
        tasks.append(load_plugin(plugin_name, plugin_settings))

    loaded_plugins = loop.run_until_complete(asyncio.gather(*tasks))
    # Filter out None results
    connectors = [plugin for plugin in loaded_plugins if plugin is not None]

    elapsed = time.time() - start_time
    logger.info(f"Loaded {len(connectors)} SaaS connector(s) in {elapsed:.2f} seconds.")
    return connectors

if __name__ == "__main__":
    # Example test configuration for SaaS connectors.
    test_config = {
        "saas": {
            "m365": {
                "enabled": True,
                "config": {
                    "api_endpoint": "https://graph.microsoft.com/v1.0/",
                    "client_id": "your-m365-client-id",
                    "client_secret": "your-m365-client-secret",
                    "tenant_id": "your-m365-tenant-id"
                    # Additional configuration...
                }
            },
            "github": {
                "enabled": True,
                "config": {
                    "api_endpoint": "https://api.github.com/",
                    "access_token": "your-github-access-token"
                    # Additional configuration...
                }
            },
            "servicenow": {
                "enabled": False,
                "config": {
                    # Servicenow configuration (if enabled)
                }
            },
            "salesforce": {
                "enabled": True,
                "config": {
                    # Salesforce configuration details.
                }
            }
        }
    }
    
    # Load connectors using the orchestrator.
    connectors = load_saas_connectors(test_config)
    # For demonstration, call fetch_identities() on each connector synchronously.
    for connector in connectors:
        try:
            identities = connector.fetch_identities()
            print(f"Identities from {connector.__class__.__name__}:")
            print(identities)
        except Exception as e:
            logger.error(f"Error fetching identities from connector {connector.__class__.__name__}: {e}")
