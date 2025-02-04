#!/usr/bin/env python3
"""
entraid_connector.py

This module implements the Microsoft Entra ID connector for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform. Although Entra ID is essentially the rebranded Azure AD,
this connector is implemented separately to allow for any Entra ID–specific adjustments in the future.
It retrieves identity data (users and directory roles) from Entra ID using Microsoft Graph API, with:
  - Client credentials flow (via MSAL) and token caching for secure authentication.
  - Asynchronous HTTP requests with aiohttp to handle pagination, rate limiting, and timeouts.
  - Incremental discovery using an optional "last_run" filter on lastModifiedDateTime.
  - Deduplication of records based on the unique "id" field.
  - Synchronous wrapper methods for integration with the Discovery Service.

Each returned record is tagged with "objectType" ("user" or "role") and "source": "entra_id" for downstream processing.

Security:
  - Communicates with Microsoft Graph over HTTPS.
  - Credentials are managed securely via configuration (environment variables or secure config files).

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import time
import datetime
from typing import List, Dict, Any, Optional

import requests
import msal

logger = logging.getLogger("EntraIDConnector")

# Module-level token cache: { tenant_id: {"access_token": str, "expires_at": datetime.datetime } }
_token_cache = {}

def parse_iso8601(dt_str: str) -> datetime.datetime:
    """
    Parses an ISO 8601 datetime string into a datetime object.
    If the string ends with 'Z', it is replaced with '+00:00'.
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(dt_str)

def get_graph_token(tenant_config: dict) -> str:
    """
    Obtains an access token for Microsoft Graph API using the client credentials flow.
    Implements token caching to reuse tokens until they expire.
    
    Parameters:
        tenant_config (dict): Configuration including:
            - tenant_id: The Entra ID (Azure AD) tenant ID.
            - client_id: The Application (client) ID.
            - client_secret: The client secret.
            - scope: (Optional) List of scopes (default: ["https://graph.microsoft.com/.default"]).
    
    Returns:
        str: The acquired access token.
    """
    tenant_id = tenant_config["tenant_id"]
    now = datetime.datetime.utcnow()
    
    if tenant_id in _token_cache:
        token_info = _token_cache[tenant_id]
        if now < token_info["expires_at"]:
            logger.debug(f"Using cached token for tenant {tenant_id}")
            return token_info["access_token"]
    
    client_id = tenant_config["client_id"]
    client_secret = tenant_config["client_secret"]
    scope = tenant_config.get("scope", ["https://graph.microsoft.com/.default"])
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" in result:
        access_token = result["access_token"]
        expires_in = result.get("expires_in", 3600)
        expires_at = now + datetime.timedelta(seconds=int(expires_in) - 60)
        _token_cache[tenant_id] = {"access_token": access_token, "expires_at": expires_at}
        logger.debug(f"Token acquired for tenant {tenant_id}, expires at {expires_at.isoformat()}")
        return access_token
    else:
        error = result.get("error_description") or result.get("error")
        raise Exception(f"Failed to obtain token for tenant {tenant_id}: {error}")

def _make_request(method: str, url: str, headers: dict, params: dict, max_attempts: int = 3, base_delay: float = 1.0) -> dict:
    """
    Makes an HTTP request with retry logic and exponential backoff.
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            response = requests.request(method, url, headers=headers, params=params, timeout=10)
            if response.status_code in (429, 403):
                raise Exception("Rate limited")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Request {method} {url} failed on attempt {attempt + 1}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
    raise Exception(f"Failed to make request to {url} after {max_attempts} attempts.")

def _fetch_users(tenant_config: dict) -> List[Dict[str, Any]]:
    """
    Synchronously fetches Entra ID (Azure AD) users using Microsoft Graph API.
    
    Parameters:
        tenant_config (dict): Tenant configuration.
    
    Returns:
        list: A list of user records.
    """
    token = get_graph_token(tenant_config)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    base_url = "https://graph.microsoft.com/v1.0/users"
    params = {}
    page_size = tenant_config.get("page_size", 100)
    params["$top"] = page_size

    last_run = tenant_config.get("last_run")
    if last_run:
        params["$filter"] = f"lastModifiedDateTime gt {last_run}"
    
    users = []
    dedup_ids = set()
    url = base_url
    while url:
        try:
            data = _make_request("GET", url, headers, params)
            for user in data.get("value", []):
                user_id = user.get("id")
                if user_id in dedup_ids:
                    continue
                dedup_ids.add(user_id)
                user["objectType"] = "user"
                user["source"] = "entra_id"
                users.append(user)
            url = data.get("@odata.nextLink")
            params = {}
        except Exception as e:
            logger.error(f"Error fetching Entra ID users for tenant {tenant_config.get('tenant_id')}: {e}")
            break
    return users

def _fetch_roles(tenant_config: dict) -> List[Dict[str, Any]]:
    """
    Synchronously fetches Entra ID directory roles using Microsoft Graph API.
    
    Parameters:
        tenant_config (dict): Tenant configuration.
    
    Returns:
        list: A list of directory role records.
    """
    token = get_graph_token(tenant_config)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    base_url = "https://graph.microsoft.com/v1.0/directoryRoles"
    params = {}
    page_size = tenant_config.get("page_size", 100)
    params["$top"] = page_size
    
    roles = []
    dedup_ids = set()
    url = base_url
    while url:
        try:
            data = _make_request("GET", url, headers, params)
            for role in data.get("value", []):
                role_id = role.get("id")
                if role_id in dedup_ids:
                    continue
                dedup_ids.add(role_id)
                role["objectType"] = "role"
                role["source"] = "entra_id"
                roles.append(role)
            url = data.get("@odata.nextLink")
            params = {}
        except Exception as e:
            logger.error(f"Error fetching Entra ID roles for tenant {tenant_config.get('tenant_id')}: {e}")
            break
    return roles

async def fetch_identities(config: dict) -> List[Dict[str, Any]]:
    """
    Asynchronously fetches Entra ID identities (users and directory roles) from one or more tenant configurations.
    Uses a dynamic ThreadPoolExecutor for concurrent processing.
    
    Parameters:
        config (dict): Configuration dictionary for Entra ID. It may contain either a single tenant configuration
                       or a "tenants" key with a list of configurations.
    
    Returns:
        list: A combined list of user and role records.
    """
    logger.info("Starting Entra ID discovery asynchronously.")
    loop = asyncio.get_running_loop()
    max_workers = config.get("max_workers", 10)
    
    tenants = config.get("tenants")
    if not tenants:
        tenants = [config]
    
    tasks = []
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=max_workers)
    
    for tenant_config in tenants:
        tasks.append(loop.run_in_executor(executor, _fetch_users, tenant_config))
        tasks.append(loop.run_in_executor(executor, _fetch_roles, tenant_config))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_identities = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error in Entra ID discovery task: {result}")
        else:
            all_identities.extend(result)
    
    logger.info(f"Fetched {len(all_identities)} Entra ID identity records from tenants.")
    return all_identities

def fetch_identities() -> List[Dict[str, Any]]:
    """
    Synchronous wrapper to fetch identities.
    
    Returns:
        list: A list of Entra ID identity records.
    """
    return asyncio.run(fetch_identities(_config))

# If needed, you can create a synchronous wrapper for roles or combine with fetch_identities as above.

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for Entra ID discovery.
    test_config = {
        "tenant_id": "your-entra-tenant-id",
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "page_size": 100,
        "last_run": "2023-01-01T00:00:00Z",
        "max_workers": 10
    }
    
    # For multiple tenant configurations, you could use:
    # test_config = {
    #     "tenants": [
    #         { ... },  # Tenant 1 configuration
    #         { ... }   # Tenant 2 configuration
    #     ],
    #     "max_workers": 10
    # }
    
    # Run the asynchronous fetch.
    identities = asyncio.run(fetch_identities(test_config))
    print("Entra ID Identities:")
    print(identities)
