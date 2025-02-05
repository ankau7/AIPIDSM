#!/usr/bin/env python3
"""
servicenow_connector.py

This module implements the ServiceNow connector for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform. It retrieves identity data (for example, user records)
from ServiceNow via its REST API using Basic Authentication.

Key Features:
  - Connects securely over HTTPS using Basic Authentication with credentials provided in the configuration.
  - Queries a specified ServiceNow table (default: "sys_user") for identity records.
  - Supports incremental discovery using a "last_run" parameter, filtering records based on the "sys_updated_on" field.
  - Paginates through results using "sysparm_limit" and "sysparm_offset" parameters.
  - Deduplicates records based on the unique "sys_id" field using an async-safe cache.
  - Uses asynchronous HTTP calls (via aiohttp) to efficiently fetch data.
  - Maps each record to a standardized identity format with fields such as "UserID", "Username", "Email", "DisplayName", and "LastModified".
  - Tags each record with "objectType": "user" and "source": "servicenow" for downstream processing.

Security:
  - All communication with ServiceNow occurs over HTTPS.
  - Credentials (instance URL, username, password) should be managed securely via configuration.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import aiohttp

from generic_saas_connector import GenericSaasConnector

logger = logging.getLogger("ServiceNowConnector")

class ServiceNowConnector(GenericSaasConnector):
    def initialize(self) -> None:
        """
        Initializes the ServiceNow connector by reading configuration parameters.
        """
        self.instance_url = self.config.get("instance_url")  # e.g., "https://yourinstance.service-now.com"
        self.username = self.config.get("username")
        self.password = self.config.get("password")
        self.table = self.config.get("table", "sys_user")
        self.sysparm_limit = self.config.get("sysparm_limit", 100)
        self.last_run = self.config.get("last_run")  # Expecting a string in "YYYY-MM-DD HH:MM:SS" format
        self.max_workers = self.config.get("max_workers", 10)
        self.api_timeout = self.config.get("api_timeout", 10)
        self.rate_limit_retry = self.config.get("rate_limit_retry", {"max_attempts": 5, "base_delay": 1.0})
        self.kafka_topics = self.config.get("kafka_topics", {"identity": "servicenow-identity"})
        self.cache = set()  # For deduplication based on sys_id
        self.lock = asyncio.Lock()
        logger.info("ServiceNowConnector initialized for table '%s' at instance '%s'.", self.table, self.instance_url)

    async def handle_rate_limit(self, response: aiohttp.ClientResponse) -> None:
        """
        Handles rate limiting by sleeping until the API indicates it can be called again.
        """
        if response.status == 429:
            retry_after = response.headers.get("Retry-After")
            sleep_time = int(retry_after) if retry_after and retry_after.isdigit() else 60
            logger.warning("ServiceNow API rate limit encountered. Sleeping for %d seconds.", sleep_time)
            await asyncio.sleep(sleep_time)
        else:
            logger.error("Unexpected rate limit response with status %d.", response.status)
            raise Exception("Rate limit encountered without proper Retry-After header.")

    async def fetch_paginated_data(self, url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches paginated data from the ServiceNow API.
        
        Parameters:
            url (str): The API endpoint URL.
            params (dict): Query parameters for the API call.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of records from the API.
        """
        results = []
        auth = aiohttp.BasicAuth(login=self.username, password=self.password)
        async with aiohttp.ClientSession(auth=auth, timeout=aiohttp.ClientTimeout(total=self.api_timeout)) as session:
            while url:
                async with session.get(url, params=params) as response:
                    if response.status == 429:
                        await self.handle_rate_limit(response)
                        continue
                    response.raise_for_status()
                    data = await response.json()
                    batch = data.get("result", [])
                    results.extend(batch)
                    # If fewer records than the limit were returned, we are at the end.
                    if len(batch) < params.get("sysparm_limit", self.sysparm_limit):
                        break
                    # Increment the offset.
                    offset = params.get("sysparm_offset", 0) + self.sysparm_limit
                    params["sysparm_offset"] = offset
        return results

    async def _async_fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches identity records from ServiceNow.
        
        Returns:
            List[Dict[str, Any]]: List of identity records.
        """
        base_url = f"{self.instance_url}/api/now/table/{self.table}"
        params = {
            "sysparm_limit": self.sysparm_limit,
            "sysparm_offset": 0
        }
        if self.last_run:
            # The sysparm_query expects a query string; adjust format as needed.
            # For example: sys_updated_on>=2023-01-01 00:00:00
            params["sysparm_query"] = f"sys_updated_on>={self.last_run}"
        
        records = await self.fetch_paginated_data(base_url, params)
        identities = []
        for record in records:
            sys_id = record.get("sys_id")
            async with self.lock:
                if sys_id in self.cache:
                    continue
                self.cache.add(sys_id)
            identity = {
                "UserID": sys_id,
                "Username": record.get("user_name"),
                "Email": record.get("email"),
                "DisplayName": record.get("name"),
                "LastModified": record.get("sys_updated_on"),
                "objectType": "user",
                "source": "servicenow"
            }
            identities.append(identity)
        logger.info("Fetched %d identity records from ServiceNow table '%s'.", len(identities), self.table)
        return identities

    def fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper for the asynchronous identity fetch.
        
        Returns:
            List[Dict[str, Any]]: List of identity records.
        """
        return asyncio.run(self._async_fetch_identities())

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for ServiceNow discovery.
    test_config = {
        "instance_url": "https://yourinstance.service-now.com",
        "username": "your_username",
        "password": "your_password",
        "table": "sys_user",
        "sysparm_limit": 100,
        "last_run": "2023-01-01 00:00:00",  # Format: "YYYY-MM-DD HH:MM:SS"
        "max_workers": 10,
        "api_timeout": 10,
        "rate_limit_retry": {
            "max_attempts": 5,
            "base_delay": 1.0
        },
        "kafka_topics": {
            "identity": "servicenow-identity"
        }
    }
    
    connector = ServiceNowConnector(test_config)
    identities = asyncio.run(connector._async_fetch_identities())
    print("ServiceNow Identities:")
    print(identities)
