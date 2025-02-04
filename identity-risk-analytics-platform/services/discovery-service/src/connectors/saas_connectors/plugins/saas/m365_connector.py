#!/usr/bin/env python3
"""
m365_connector.py

This module implements the Microsoft 365 (M365) discovery connector for the Discovery Service
of the AI-Powered Identity Risk Analytics Platform. It is designed for retrieving identity data from M365
via Microsoft Graph API using client credentials (via MSAL) and asynchronous HTTP calls.

Key Features:
  - Obtains an access token from Microsoft Graph using MSAL, with token caching.
  - Asynchronously fetches user identities from the /v1.0/users endpoint.
  - Supports incremental discovery by applying a filter on lastModifiedDateTime (if "last_run" is provided).
  - Handles pagination via the @odata.nextLink header.
  - Implements rate limit handling by pausing when a 429/403 response is encountered.
  - Deduplicates records based on the unique "id" field.
  - Maps each record with "objectType": "user" and "source": "m365" for downstream processing.

Security:
  - All communication with Microsoft Graph occurs over HTTPS.
  - Credentials (tenant_id, client_id, client_secret) are managed securely via configuration.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import time
import datetime
from typing import List, Dict, Any, Optional

import aiohttp
import msal

# Import our generic SaaS connector base class
from generic_saas_connector import GenericSaasConnector

logger = logging.getLogger("M365Connector")

class M365Connector(GenericSaasConnector):
    def initialize(self) -> None:
        """
        Initializes the M365 connector by reading configuration and performing authentication.
        Sets up token caching and connection parameters.
        """
        self.tenant_id = self.config.get("tenant_id")
        self.client_id = self.config.get("client_id")
        self.client_secret = self.config.get("client_secret")
        self.api_endpoint = self.config.get("api_endpoint", "https://graph.microsoft.com/v1.0/")
        self.organization = self.config.get("organization")  # Optional, may be used for filtering
        self.max_workers = self.config.get("max_workers", 10)
        self.api_timeout = self.config.get("api_timeout", 10)
        self.rate_limit_retry = self.config.get("rate_limit_retry", {"max_attempts": 5, "base_delay": 1.0})
        # Optional incremental discovery: last_run in ISO 8601 format
        self.last_run = self.config.get("last_run")
        
        # Kafka topics may be defined in the configuration if needed downstream
        self.kafka_topics = self.config.get("kafka_topics", {"identity": "m365-identity"})
        
        # Token caching: store the access token and its expiry
        self._token_cache: Dict[str, Dict[str, Any]] = {}
        
        # Async-safe deduplication cache and lock.
        self.cache = set()
        self.lock = asyncio.Lock()
        
        self.logger.info("M365Connector initialized.")

    def get_auth_token(self) -> str:
        """
        Retrieves an access token from Microsoft Graph using client credentials.
        Implements token caching to avoid repeated token requests.
        
        Returns:
            str: The access token.
        """
        now = datetime.datetime.utcnow()
        cache_key = self.tenant_id
        if cache_key in self._token_cache:
            token_info = self._token_cache[cache_key]
            if now < token_info["expires_at"]:
                logger.debug("Using cached token for tenant %s", self.tenant_id)
                return token_info["access_token"]
        
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        scope = self.config.get("scope", ["https://graph.microsoft.com/.default"])
        
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )
        result = app.acquire_token_for_client(scopes=scope)
        if "access_token" in result:
            access_token = result["access_token"]
            expires_in = int(result.get("expires_in", 3600))
            expires_at = now + datetime.timedelta(seconds=expires_in - 60)
            self._token_cache[cache_key] = {"access_token": access_token, "expires_at": expires_at}
            logger.debug("Acquired new token for tenant %s; expires at %s", self.tenant_id, expires_at.isoformat())
            return access_token
        else:
            error = result.get("error_description") or result.get("error")
            raise Exception(f"Failed to obtain token for tenant {self.tenant_id}: {error}")

    async def handle_rate_limit(self, response: aiohttp.ClientResponse) -> None:
        """
        Handles rate limiting responses from Microsoft Graph by sleeping until reset.
        
        Parameters:
            response (aiohttp.ClientResponse): The response object.
        """
        if response.status in (429, 403):
            reset = response.headers.get("Retry-After")  # Microsoft Graph may provide Retry-After
            if reset:
                try:
                    sleep_time = int(reset)
                except ValueError:
                    sleep_time = 60
            else:
                sleep_time = 60
            logger.warning("Rate limit encountered. Sleeping for %s seconds.", sleep_time)
            await asyncio.sleep(sleep_time)
        else:
            logger.error("Unexpected rate limit response without Retry-After header.")
            raise Exception("Rate limited without a clear reset time.")

    async def fetch_paginated_data(self, url: str) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches paginated data from the specified Microsoft Graph API endpoint.
        
        Parameters:
            url (str): The starting URL for the request.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of results.
        """
        data = []
        headers = {
            "Authorization": f"Bearer {self.get_auth_token()}",
            "Content-Type": "application/json"
        }
        params = {"$top": self.config.get("page_size", 100)}
        if self.last_run:
            # Filter on lastModifiedDateTime (ISO 8601 format)
            params["$filter"] = f"lastModifiedDateTime gt {self.last_run}"
        
        async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=self.api_timeout)) as session:
            while url:
                async with session.get(url, params=params) as response:
                    if response.status in (429, 403):
                        await self.handle_rate_limit(response)
                        continue
                    response.raise_for_status()
                    page_data = await response.json()
                    # The response for users typically contains a "value" array.
                    data.extend(page_data.get("value", []))
                    url = page_data.get("@odata.nextLink")
                    params = {}  # NextLink already includes query parameters.
        return data

    async def _async_fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches M365 identity data (users) from Microsoft Graph.
        
        Returns:
            List[Dict[str, Any]]: List of user records.
        """
        url = f"{self.api_endpoint}users"
        users = await self.fetch_paginated_data(url)
        identities = []
        for user in users:
            record_id = user.get("id")
            async with self.lock:
                if record_id in self.cache:
                    continue
                self.cache.add(record_id)
            # Map relevant fields; additional Office 365 fields can be added as needed.
            identity = {
                "UserID": record_id,
                "Username": user.get("userPrincipalName") or user.get("displayName"),
                "Email": user.get("mail"),
                "JobTitle": user.get("jobTitle"),
                "Department": user.get("department"),
                "OfficeLocation": user.get("officeLocation"),
                "LastModified": user.get("lastModifiedDateTime"),
                "objectType": "user",
                "source": "m365"
            }
            identities.append(identity)
        logger.info("Fetched %d identities from M365.", len(identities))
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
    
    # Example configuration for M365 discovery.
    test_config = {
        "tenant_id": "your-m365-tenant-id",
        "client_id": "your-m365-client-id",
        "client_secret": "your-m365-client-secret",
        "organization": "your_m365_organization",  # Optional for filtering, if needed.
        "api_endpoint": "https://graph.microsoft.com/v1.0/",
        "page_size": 100,
        "last_run": "2023-01-01T00:00:00Z",
        "max_workers": 10,
        "rate_limit_retry": {
            "max_attempts": 5,
            "base_delay": 1.0
        },
        "kafka_topics": {
            "identity": "m365-identity"
        }
    }
    
    connector = M365Connector(test_config)
    identities = connector.fetch_identities()
    print("M365 Identities:")
    print(identities)
