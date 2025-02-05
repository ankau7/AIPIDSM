#!/usr/bin/env python3
"""
ad_resource_connector.py

This module implements the Active Directory Resource Discovery Connector for the
AI-Powered Identity Risk Analytics Platform. It uses LDAPS with certificate‐based
authentication (instead of using a plain-text password) to securely query AD for objects
representing resources such as shared drives (volumes), printers, Exchange resources, and
Group Policy Objects (GPOs). The connector captures extended attributes (including ACL data)
and supports incremental discovery via the "whenChanged" attribute.

Discovered resource records are mapped into a standardized format for downstream processing,
enabling comprehensive graph correlation and risk analytics.

Security:
  - Uses secure LDAPS connections with client certificate authentication.
  - Sensitive credentials (client certificate and key) should be managed securely (e.g., via environment variables or a secret manager).

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from ldap3 import Server, Connection, ALL, SUBTREE

logger = logging.getLogger("ADResourceConnector")
logger.setLevel(logging.DEBUG)

class ADResourceConnector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the AD Resource Discovery Connector with the provided configuration.

        Expected configuration keys:
          - ldap_server: LDAPS server URL (e.g., "ldaps://ad.example.com")
          - base_dn: Base distinguished name for searches (e.g., "DC=example,DC=com")
          - client_certificate: Path to (or contents of) the client certificate for authentication.
          - client_key: (Optional) Path to (or contents of) the client private key, if required.
          - resource_classes: List of object classes to search for (default: ["volume", "printQueue", "msExchResource", "groupPolicyContainer"])
          - attributes: List of attributes to retrieve (default: ["cn", "distinguishedName", "whenCreated", "whenChanged", "description", "nTSecurityDescriptor"])
          - last_run: (Optional) LDAP generalized time string (e.g., "20230101000000Z") to filter for objects modified after this time.
          - page_size: Number of records per page (default: 1000)
          - kafka_topics: (Optional) Mapping for Kafka topics (e.g., {"resource": "ad-resource"})
        """
        self.config = config
        self.ldap_server = config.get("ldap_server")
        self.base_dn = config.get("base_dn")
        self.client_certificate = config.get("client_certificate")
        self.client_key = config.get("client_key")  # Optional; if not provided, the certificate may include the key.
        self.resource_classes = config.get("resource_classes", ["volume", "printQueue", "msExchResource", "groupPolicyContainer"])
        self.attributes = config.get("attributes", ["cn", "distinguishedName", "whenCreated", "whenChanged", "description", "nTSecurityDescriptor"])
        self.last_run = config.get("last_run")  # e.g., "20230101000000Z"
        self.page_size = config.get("page_size", 1000)
        self.kafka_topics = config.get("kafka_topics", {"resource": "ad-resource"})
        
        # Deduplication: we use the distinguishedName as the unique key.
        self.cache = set()
        self.lock = asyncio.Lock()
        
        logger.info("ADResourceConnector initialized with server '%s' and base DN '%s'.", self.ldap_server, self.base_dn)

    def _build_search_filter(self) -> str:
        """
        Constructs an LDAP search filter to retrieve resource objects.
        Combines desired object classes with an OR filter and applies a whenChanged filter for incremental discovery if provided.
        
        Returns:
            str: The LDAP search filter.
        """
        # Create an OR filter for each resource class.
        class_filters = "".join(f"(objectClass={cls})" for cls in self.resource_classes)
        base_filter = f"(|{class_filters})"
        if self.last_run:
            full_filter = f"(&{base_filter}(whenChanged>={self.last_run}))"
        else:
            full_filter = base_filter
        logger.debug("Constructed LDAP search filter: %s", full_filter)
        return full_filter

    def _fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Synchronously fetches resource objects from Active Directory using LDAP paging.
        
        Returns:
            List[Dict[str, Any]]: A list of standardized resource records.
        """
        resources = []
        try:
            # Create the Server object with client certificate-based authentication.
            server = Server(self.ldap_server, use_ssl=True, get_info=ALL,
                            client_certificate=self.client_certificate,
                            client_key=self.client_key)
            # For certificate-based authentication, we'll use a simple bind.
            conn = Connection(server, auto_bind=True)
            search_filter = self._build_search_filter()
            cookie = None
            
            while True:
                conn.search(
                    search_base=self.base_dn,
                    search_filter=search_filter,
                    search_scope=SUBTREE,
                    attributes=self.attributes,
                    paged_size=self.page_size,
                    paged_cookie=cookie
                )
                for entry in conn.entries:
                    entry_json = entry.entry_to_json()
                    entry_dict = json.loads(entry_json)
                    # Use distinguishedName as the unique identifier.
                    dn = entry_dict.get("dn") or entry_dict.get("attributes", {}).get("distinguishedName")
                    if not dn:
                        continue
                    if dn in self.cache:
                        continue
                    self.cache.add(dn)
                    
                    attributes = entry_dict.get("attributes", {})
                    resource = {
                        "ResourceID": dn,
                        "ResourceName": attributes.get("cn"),
                        "ResourceType": self._determine_resource_type(attributes),
                        "UNCPath": attributes.get("unCName"),  # May be applicable for shared drives.
                        "Description": attributes.get("description"),
                        "CreatedDate": attributes.get("whenCreated"),
                        "LastModifiedDate": attributes.get("whenChanged"),
                        "ACL": attributes.get("nTSecurityDescriptor"),  # Raw ACL data, if available.
                        "objectType": "resource",
                        "source": "ad"
                    }
                    resources.append(resource)
                # Handle paging.
                controls = conn.result.get("controls", {})
                page_control = controls.get("1.2.840.113556.1.4.319", {})
                cookie = page_control.get("value", {}).get("cookie")
                if not cookie:
                    break
            conn.unbind()
        except Exception as e:
            logger.error("Error fetching resources from AD: %s", e)
        return resources

    def _determine_resource_type(self, attributes: Dict[str, Any]) -> str:
        """
        Determines the resource type based on the object's attributes.
        
        Parameters:
            attributes (dict): LDAP attributes of the resource.
        
        Returns:
            str: The resource type (e.g., "volume", "printer", "exchange_resource", "gpo", or "unknown").
        """
        obj_classes = attributes.get("objectClass", [])
        if isinstance(obj_classes, str):
            obj_classes = [obj_classes]
        if "volume" in obj_classes:
            return "volume"
        elif "printQueue" in obj_classes:
            return "printer"
        elif "msExchResource" in obj_classes:
            return "exchange_resource"
        elif "groupPolicyContainer" in obj_classes:
            return "gpo"
        else:
            return "unknown"

    async def _async_fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches resource objects using a ThreadPoolExecutor.
        
        Returns:
            List[Dict[str, Any]]: A list of resource records.
        """
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=self.config.get("max_workers", 10))
        resources = await loop.run_in_executor(executor, self._fetch_resources)
        logger.info("Fetched %d resource records from AD.", len(resources))
        return resources

    def fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper for asynchronous resource fetching.
        
        Returns:
            List[Dict[str, Any]]: List of resource records.
        """
        return asyncio.run(self._async_fetch_resources())

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for AD resource discovery using certificate-based authentication.
    test_config = {
        "ldap_server": "ldaps://ad.example.com",
        "base_dn": "DC=example,DC=com",
        # Instead of ldap_password, use client certificate information.
        "client_certificate": "/path/to/your/client_certificate.pem",
        "client_key": "/path/to/your/client_key.pem",  # Optional if the certificate file contains the key.
        "resource_classes": ["volume", "printQueue", "msExchResource", "groupPolicyContainer"],
        "attributes": ["cn", "distinguishedName", "whenCreated", "whenChanged", "description", "nTSecurityDescriptor"],
        "last_run": "20230101000000Z",
        "page_size": 1000,
        "max_workers": 10,
        "kafka_topics": {
            "resource": "ad-resource"
        }
    }
    
    connector = ADResourceConnector(test_config)
    resources = connector.fetch_resources()
    print("AD Resource Records:")
    print(resources)
