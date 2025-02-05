#!/usr/bin/env python3
"""
servicenow_connector.py

This module implements the ServiceNow connector for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform using OAuth 2.0 for secure authentication.
It retrieves identity data (for example, user records from the "sys_user" table)
from a ServiceNow instance via its REST API.

Key Features:
  - Authenticates with ServiceNow using OAuth 2.0 (client credentials flow) instead of Basic Auth.
  - Obtains and caches an access token until near expiration.
  - Retrieves records from a specified ServiceNow table (default: "sys_user").
  - Supports incremental discovery via a "last_run" filter on the "sys_updated_on" field.
  - Uses pagination via "sysparm_limit" and "sysparm_offset".
  - Deduplicates records based on the unique "sys_id" field using an async-safe cache.
  - Makes asynchronous HTTP requests with aiohttp, including rate limit handling.

Security:
  - All communications with ServiceNow occur over HTTPS.
  - OAuth credentials (client_id, client_secret, token_endpoint) are provided in configuration and should be managed securely.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

import aiohttp
import requests

from generic_saas_connector import GenericSaasConnector

logger = logging.getLogger("ServiceNowConnector")

# Module-level token cache: { instance_url: {"access_token": str, "expires_at": datetime} }
_token_cache = {}

class ServiceNowConnector(GenericSaasConnector):
    def initialize(self) -> None:
        """
        Initializes the ServiceNow connector by reading configuration parameters.
        This version uses OAuth 2.0 for authentication.
        """
        self.instance_url = self.config.get("instance_url")  # e.g., "https://yourinstance.service-now.com"
        self.client_id = self.config.get("client_id")
        self.client_secret = self.config.get("client_secret")
        self.token_endpoint = self.config.get("token_endpoint", f"{self.instance_url}/oauth_token.do")
        self.table = self.config.get("table", "sys_user")
        self.sysparm_limit = self.config.get("sysparm_limit", 100)
        self.last_run = self.config.get("last_run")  # Format: "YYYY-MM-DD HH:MM:SS"
        self.max_workers = self.config.get("max_workers", 10)
        self.api_timeout = self.config.get("api_timeout", 10)
        self.rate_limit_retry = self.config.get("rate_limit_retry", {"max_attempts": 5, "base_delay": 1.0})
        self.kafka_topics = self.config.get("kafka_topics", {"identity": "servicenow-identity"})
        
        # Async-safe deduplication cache and lock.
        self.cache = set()  # For deduplication based on sys_id
        self.lock = asyncio.Lock()
        
        logger.info("ServiceNowConnector initialized for table '%s' at instance '%s' using OAuth.", self.table, self.instance_url)

    def get_auth_token(self) -> str:
        """
        Obtains an access token from ServiceNow using OAuth 2.0 client credentials flow.
        Implements token caching to reuse the token until it is near expiration.
        
        Returns:
            str: The access token.
        """
        now = datetime.utcnow()
        cache_key = self.instance_url
        if cache_key in _token_cache:
            token_info = _token_cache[cache_key]
            if now < token_info["expires_at"]:
                logger.debug("Using cached token for instance %s", self.instance_url)
                return token_info["access_token"]
        
        # Prepare the token request.
        token_url = self.token_endpoint
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        try:
            response = requests.post(token_url, data=payload, headers=headers, timeout=10)
            response.raise_for_status()
            result = response.json()
            if "access_token" in result:
                access_token = result["access_token"]
                expires_in = int(result.get("expires_in", 3600))
                expires_at = now + timedelta(seconds=expires_in - 60)  # Subtract 60 seconds to be safe
                _token_cache[cache_key] = {"access_token": access_token, "expires_at": expires_at}
                logger.debug("Acquired new token for instance %s; expires at %s", self.instance_url, expires_at.isoformat())
                return access_token
            else:
                error = result.get("error_description") or result.get("error")
                raise Exception(f"Token request failed: {error}")
        except Exception as e:
            logger.error("Error obtaining OAuth token from ServiceNow: %s", e)
            raise

    async def handle_rate_limit(self, response: aiohttp.ClientResponse) -> None:
        """
        Handles ServiceNow API rate limiting by sleeping until the reset time.
        
        Parameters:
            response (aiohttp.ClientResponse): The response object.
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
            url (str): The ServiceNow API endpoint URL.
            params (dict): Query parameters for the API call.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of records.
        """
        results = []
        token = self.get_auth_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        auth = None  # Not using BasicAuth; we're using OAuth Bearer token.
        async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=self.api_timeout)) as session:
            while url:
                async with session.get(url, params=params) as response:
                    if response.status == 429:
                        await self.handle_rate_limit(response)
                        continue
                    response.raise_for_status()
                    data = await response.json()
                    batch = data.get("result", [])
                    results.extend(batch)
                    # If fewer records than the limit were returned, assume it's the last page.
                    if len(batch) < params.get("sysparm_limit", self.sysparm_limit):
                        break
                    # Increment the offset.
                    offset = params.get("sysparm_offset", 0) + self.sysparm_limit
                    params["sysparm_offset"] = offset
        return results

    async def _async_fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches identity records from ServiceNow using OAuth 2.0.
        
        Returns:
            List[Dict[str, Any]]: A list of mapped identity records.
        """
        base_url = f"{self.instance_url}/api/now/table/{self.table}"
        params = {
            "sysparm_limit": self.sysparm_limit,
            "sysparm_offset": 0
        }
        if self.last_run:
            # Query format: sys_updated_on>=YYYY-MM-DD HH:MM:SS
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
        Synchronous wrapper for asynchronous identity fetching.
        
        Returns:
            List[Dict[str, Any]]: List of identity records.
        """
        return asyncio.run(self._async_fetch_identities())

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for ServiceNow discovery using OAuth.
    test_config = {
        "instance_url": "https://yourinstance.service-now.com",
        "client_id": "your_servicenow_client_id",
        "client_secret": "your_servicenow_client_secret",
        "token_endpoint": "https://yourinstance.service-now.com/oauth_token.do",
        "table": "sys_user",
        "sysparm_limit": 100,
        "last_run": "2023-01-01 00:00:00",
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
