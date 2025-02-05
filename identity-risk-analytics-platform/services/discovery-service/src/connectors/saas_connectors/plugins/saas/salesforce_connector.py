#!/usr/bin/env python3
"""
salesforce_connector.py

This module implements the Salesforce connector for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform. It retrieves identity data (user records)
from Salesforce via its REST API using OAuth 2.0 authentication. For demonstration,
this implementation uses the username–password OAuth 2.0 flow with token caching,
but a more secure flow (such as JWT Bearer) is recommended for production.

Key Features:
  - Obtains an access token from Salesforce using OAuth 2.0 (username–password grant type) with token caching.
  - Asynchronously executes a SOQL query to fetch user records.
  - Supports pagination via the "nextRecordsUrl" property.
  - Optionally filters for incremental discovery if a "last_run" timestamp is provided (by appending a condition to the query).
  - Deduplicates records based on the unique "Id" field.
  - Maps each record to a standardized identity format with keys such as "UserID", "Username", "Email", "DisplayName", and "LastModified".
  - Tags each record with "objectType": "user" and "source": "salesforce".

Security:
  - All communications with Salesforce occur over HTTPS.
  - OAuth credentials (client_id, client_secret, username, password) are provided securely via configuration (or ideally via a secret manager).
  
Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import time
import datetime
from datetime import timedelta
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import requests

from generic_saas_connector import GenericSaasConnector

logger = logging.getLogger("SalesforceConnector")

# Module-level token cache: { instance_url: {"access_token": str, "expires_at": datetime.datetime } }
_token_cache = {}

def parse_iso8601(dt_str: str) -> datetime.datetime:
    """
    Parses an ISO 8601 datetime string into a datetime object.
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(dt_str)

class SalesforceConnector(GenericSaasConnector):
    def initialize(self) -> None:
        """
        Initializes the Salesforce connector by reading configuration parameters.
        """
        self.instance_url = self.config.get("instance_url")  # e.g., "https://yourinstance.my.salesforce.com"
        self.token_endpoint = self.config.get("token_endpoint", "https://login.salesforce.com/services/oauth2/token")
        # For demonstration, we use the "password" grant type.
        self.client_id = self.config.get("client_id")
        self.client_secret = self.config.get("client_secret")
        self.username = self.config.get("username")
        self.password = self.config.get("password")  # Typically, password concatenated with security token.
        # SOQL query to fetch user records. It should select required fields.
        self.query = self.config.get("query", "SELECT Id, Username, Email, Name, LastModifiedDate FROM User")
        # Optional incremental discovery: last_run in ISO 8601 format.
        self.last_run = self.config.get("last_run")
        self.max_workers = self.config.get("max_workers", 10)
        self.api_timeout = self.config.get("api_timeout", 10)
        self.rate_limit_retry = self.config.get("rate_limit_retry", {"max_attempts": 5, "base_delay": 1.0})
        self.kafka_topics = self.config.get("kafka_topics", {"identity": "salesforce-identity"})
        self.cache = set()  # For deduplication based on Salesforce "Id"
        self.lock = asyncio.Lock()
        logger.info("SalesforceConnector initialized for instance '%s'.", self.instance_url)

    def get_auth_token(self) -> str:
        """
        Obtains an access token from Salesforce using the OAuth 2.0 password grant.
        Caches the token until it is near expiration.
        
        Returns:
            str: The access token.
        """
        now = datetime.datetime.utcnow()
        cache_key = self.instance_url
        if cache_key in _token_cache:
            token_info = _token_cache[cache_key]
            if now < token_info["expires_at"]:
                logger.debug("Using cached token for instance %s", self.instance_url)
                return token_info["access_token"]
        
        token_url = self.token_endpoint
        payload = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        try:
            response = requests.post(token_url, data=payload, headers=headers, timeout=10)
            response.raise_for_status()
            result = response.json()
            if "access_token" in result:
                access_token = result["access_token"]
                # Salesforce tokens typically expire in 3600 seconds (1 hour); adjust as needed.
                expires_in = int(result.get("expires_in", 3600))
                expires_at = now + timedelta(seconds=expires_in - 60)
                _token_cache[cache_key] = {"access_token": access_token, "expires_at": expires_at}
                logger.debug("Acquired new token for instance %s; expires at %s", self.instance_url, expires_at.isoformat())
                return access_token
            else:
                error = result.get("error_description") or result.get("error")
                raise Exception(f"Token request failed: {error}")
        except Exception as e:
            logger.error("Error obtaining OAuth token from Salesforce: %s", e)
            raise

    async def handle_rate_limit(self, response: aiohttp.ClientResponse) -> None:
        """
        Handles rate limiting responses from Salesforce by sleeping for the duration specified in the Retry-After header.
        """
        if response.status == 429:
            retry_after = response.headers.get("Retry-After")
            sleep_time = int(retry_after) if retry_after and retry_after.isdigit() else 60
            logger.warning("Salesforce API rate limit encountered. Sleeping for %d seconds.", sleep_time)
            await asyncio.sleep(sleep_time)
        else:
            logger.error("Unexpected rate limit response with status %d.", response.status)
            raise Exception("Rate limit encountered without a proper Retry-After header.")

    async def fetch_paginated_data(self, url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches paginated data from the Salesforce query API.
        
        Salesforce uses the "nextRecordsUrl" property for pagination.
        
        Parameters:
            url (str): The initial URL for the query.
            params (dict): Query parameters for the API call.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of query records.
        """
        results = []
        headers = {
            "Authorization": f"Bearer {self.get_auth_token()}",
            "Content-Type": "application/json"
        }
        async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=self.api_timeout)) as session:
            while url:
                async with session.get(url, params=params) as response:
                    if response.status == 429:
                        await self.handle_rate_limit(response)
                        continue
                    response.raise_for_status()
                    data = await response.json()
                    # The query response includes "records" and may include "nextRecordsUrl".
                    batch = data.get("records", [])
                    results.extend(batch)
                    url = data.get("nextRecordsUrl")
                    params = {}  # NextRecordsUrl already includes query parameters.
        return results

    async def _async_fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches identity records from Salesforce.
        Executes a SOQL query (with optional incremental filtering) against the Salesforce REST API.
        
        Returns:
            List[Dict[str, Any]]: List of mapped identity records.
        """
        # Build the query URL.
        base_url = f"{self.instance_url}/services/data/vXX.X/query/"  # Replace XX.X with the API version, e.g., v53.0.
        # Use the query from configuration; if last_run is provided, append an additional filter.
        soql = self.query
        if self.last_run:
            # Append condition to filter by LastModifiedDate.
            # Note: Ensure the query syntax is valid SOQL (this may require adjusting based on your schema).
            soql += f" WHERE LastModifiedDate > {self.last_run}"
        params = {"q": soql, "pageSize": self.config.get("page_size", 100)}
        
        records = await self.fetch_paginated_data(base_url, params)
        identities = []
        for record in records:
            record_id = record.get("Id")
            async with self.lock:
                if record_id in self.cache:
                    continue
                self.cache.add(record_id)
            identity = {
                "UserID": record_id,
                "Username": record.get("Username") or record.get("Name"),
                "Email": record.get("Email"),
                "DisplayName": record.get("Name"),
                "LastModified": record.get("LastModifiedDate"),
                "objectType": "user",
                "source": "salesforce"
            }
            identities.append(identity)
        logger.info("Fetched %d identity records from Salesforce.", len(identities))
        return identities

    def fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper to fetch identities.
        
        Returns:
            List[Dict[str, Any]]: List of identity records.
        """
        return asyncio.run(self._async_fetch_identities())

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for Salesforce discovery.
    test_config = {
        "instance_url": "https://yourinstance.my.salesforce.com",
        "token_endpoint": "https://login.salesforce.com/services/oauth2/token",
        "client_id": "your_salesforce_client_id",
        "client_secret": "your_salesforce_client_secret",
        "username": "your_salesforce_username",
        "password": "your_salesforce_password",  # Include security token if required.
        "query": "SELECT Id, Username, Email, Name, LastModifiedDate FROM User",
        "page_size": 100,
        "last_run": "2023-01-01T00:00:00Z",  # ISO 8601 format; adjust as needed.
        "max_workers": 10,
        "api_timeout": 10,
        "rate_limit_retry": {
            "max_attempts": 5,
            "base_delay": 1.0
        },
        "kafka_topics": {
            "identity": "salesforce-identity"
        }
    }
    
    connector = SalesforceConnector(test_config)
    identities = connector.fetch_identities()
    print("Salesforce Identities:")
    print(identities)
