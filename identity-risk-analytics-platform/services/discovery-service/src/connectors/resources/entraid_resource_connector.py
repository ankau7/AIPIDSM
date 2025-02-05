#!/usr/bin/env python3
"""
entraid_resource_connector.py

This module implements the Microsoft Entra ID Resource Connector for the AI-Powered Identity Risk Analytics Platform.
It retrieves application objects from Microsoft Entra ID (formerly Azure AD) using the Microsoft Graph API.
These application objects are considered "resources" (e.g., app registrations) and are mapped into a standardized format
to support risk analytics, graph correlation, and lateral movement detection.

Features:
  - Authenticates securely using OAuth 2.0 (client credentials flow) with MSAL and caches tokens until near expiration.
  - Fetches application objects from the /applications endpoint.
  - Supports incremental discovery by applying an optional filter on lastModifiedDateTime if a "last_run" timestamp is provided.
  - Handles pagination via the @odata.nextLink mechanism.
  - Deduplicates records based on the unique application ID.
  - Maps each application into a standardized resource record with key attributes.
  - Provides asynchronous processing via a ThreadPoolExecutor and a synchronous wrapper for integration.

Security:
  - All communications with Microsoft Graph occur over HTTPS.
  - Credentials (tenant_id, client_id, client_secret) are provided securely via configuration or environment variables.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import datetime
from datetime import timedelta
from typing import List, Dict, Any

import msal
import requests

logger = logging.getLogger("EntraIDResourceConnector")
logger.setLevel(logging.DEBUG)

# Module-level token cache: { tenant_id: {"access_token": str, "expires_at": datetime} }
_token_cache: Dict[str, Dict[str, Any]] = {}

def parse_iso8601(dt_str: str) -> datetime.datetime:
    """
    Parses an ISO 8601 datetime string into a datetime object.
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(dt_str)

class EntraIDResourceConnector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the Entra ID Resource Connector with the provided configuration.

        Expected configuration keys:
          - tenant_id: The Microsoft Entra ID (Azure AD) tenant ID.
          - client_id: The Application (client) ID.
          - client_secret: The client secret.
          - last_run: (Optional) ISO 8601 timestamp to filter applications modified after this time.
          - page_size: (Optional) Number of records per page (default: 100).
          - max_workers: (Optional) Maximum number of concurrent workers (default: 10).
          - kafka_topics: (Optional) Mapping for Kafka topics (e.g., {"resource": "entraid-resource"}).

        This connector retrieves application objects from the /applications endpoint.
        """
        self.config = config
        self.tenant_id = config.get("tenant_id")
        self.client_id = config.get("client_id")
        self.client_secret = config.get("client_secret")
        self.last_run = config.get("last_run")  # Optional incremental discovery filter.
        self.page_size = config.get("page_size", 100)
        self.max_workers = config.get("max_workers", 10)
        self.kafka_topics = config.get("kafka_topics", {"resource": "entraid-resource"})
        
        # Deduplication cache for application IDs.
        self.cache = set()
        
        # Set the base URL for Microsoft Graph.
        self.graph_endpoint = "https://graph.microsoft.com/v1.0/"
        
        logger.info("EntraIDResourceConnector initialized for tenant %s", self.tenant_id)

    def get_auth_token(self) -> str:
        """
        Obtains an access token for Microsoft Graph API using client credentials.
        Caches the token until it is near expiration.

        Returns:
            str: The access token.
        """
        now = datetime.datetime.utcnow()
        if self.tenant_id in _token_cache:
            token_info = _token_cache[self.tenant_id]
            if now < token_info["expires_at"]:
                logger.debug("Using cached token for tenant %s", self.tenant_id)
                return token_info["access_token"]

        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )
        result = app.acquire_token_for_client(scopes=scope)
        if "access_token" in result:
            access_token = result["access_token"]
            expires_in = int(result.get("expires_in", 3600))
            expires_at = now + timedelta(seconds=expires_in - 60)
            _token_cache[self.tenant_id] = {"access_token": access_token, "expires_at": expires_at}
            logger.debug("Acquired new token for tenant %s; expires at %s", self.tenant_id, expires_at.isoformat())
            return access_token
        else:
            error = result.get("error_description") or result.get("error")
            raise Exception(f"Failed to obtain token for tenant {self.tenant_id}: {error}")

    def _build_query_params(self) -> Dict[str, Any]:
        """
        Constructs query parameters for the applications endpoint.
        Sets the $top value to page_size.
        If last_run is provided, adds an OData filter for lastModifiedDateTime.

        Returns:
            dict: The query parameters.
        """
        params = {"$top": self.page_size}
        if self.last_run:
            params["$filter"] = f"lastModifiedDateTime gt {self.last_run}"
        logger.debug("Built query parameters for applications: %s", params)
        return params

    def _fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Synchronously fetches application objects from Microsoft Graph's /applications endpoint.
        Handles pagination via the @odata.nextLink mechanism and deduplicates records.

        Returns:
            List[Dict[str, Any]]: A list of standardized resource records.
        """
        resources = []
        token = self.get_auth_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        url = self.graph_endpoint + "applications"
        params = self._build_query_params()

        try:
            while url:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                for app in data.get("value", []):
                    app_id = app.get("id")
                    if not app_id:
                        continue
                    if app_id in self.cache:
                        continue
                    self.cache.add(app_id)
                    resource = {
                        "ResourceID": app_id,
                        "ResourceName": app.get("displayName") or app.get("name") or app_id,
                        "ResourceType": "application",  # Mark these as application resources.
                        "Description": app.get("description"),
                        "CreatedDate": app.get("createdDateTime"),
                        "LastModifiedDate": app.get("lastModifiedDateTime"),
                        "objectType": "resource",
                        "source": "entra_id"
                    }
                    resources.append(resource)
                url = data.get("@odata.nextLink")
                params = {}  # NextLink contains query parameters.
        except Exception as e:
            logger.error("Error fetching Entra ID resources (applications): %s", e)
        return resources

    async def _async_fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches application objects using a ThreadPoolExecutor.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of resource records.
        """
        loop = asyncio.get_running_loop()
        from concurrent.futures import ThreadPoolExecutor
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        resources = await loop.run_in_executor(executor, self._fetch_resources)
        logger.info("Fetched %d Entra ID resource records from applications.", len(resources))
        return resources

    def fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper for asynchronous resource fetching.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of resource records.
        """
        return asyncio.run(self._async_fetch_resources())

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for Entra ID resource discovery (applications).
    test_config = {
        "tenant_id": "your-entra-tenant-id",
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "last_run": "2023-01-01T00:00:00Z",  # Optional incremental filter.
        "page_size": 100,
        "max_workers": 10,
        "kafka_topics": {
            "resource": "entraid-resource"
        }
    }
    
    connector = EntraIDResourceConnector(test_config)
    resources = connector.fetch_resources()
    print("Entra ID Resource Records (Applications):")
    print(resources)
