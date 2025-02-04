#!/usr/bin/env python3
"""
azure_connector.py

This module implements the Azure AD connector for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform. It retrieves identity data from Azure Active Directory
using the Microsoft Graph API. The connector supports:
  - Fetching user objects via /v1.0/users (with optional filtering using lastModifiedDateTime for incremental discovery)
  - Fetching directory roles via /v1.0/directoryRoles
  - Multiple tenant support by processing a list of tenant configurations concurrently
  - Token caching to avoid fetching a new token for each API call (tokens are cached until expiry)
  - Retry logic with exponential backoff for HTTP requests to handle rate limiting and transient errors
  - Pagination using @odata.nextLink for retrieving all records

Each record is tagged with "objectType" ("user" or "role") and "source": "azure" for downstream processing.

Security:
  - Uses MSAL for secure token acquisition (client credentials flow)
  - All communication with Microsoft Graph API occurs over HTTPS
  - Credentials are managed securely via environment variables or secure configuration

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import json
import datetime
import time
from concurrent.futures import ThreadPoolExecutor

import requests
import msal

logger = logging.getLogger("AzureConnector")

# Module-level token cache: { tenant_id: {"access_token": str, "expires_at": datetime.datetime } }
_token_cache = {}

def get_graph_token(tenant_config: dict) -> str:
    """
    Obtains an access token for Microsoft Graph API using the client credentials flow.
    Implements token caching to reuse tokens until they expire.
    
    Parameters:
        tenant_config (dict): Tenant configuration including:
            - tenant_id: The Azure AD tenant ID.
            - client_id: The Application (client) ID.
            - client_secret: The client secret.
            - scope: Optional list of scopes (default: ["https://graph.microsoft.com/.default"])
    
    Returns:
        str: The acquired access token.
    """
    tenant_id = tenant_config["tenant_id"]
    now = datetime.datetime.utcnow()
    
    # Check if a valid token is already cached.
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
        # Calculate expiration time. MSAL returns "expires_in" in seconds.
        expires_in = result.get("expires_in", 3600)
        expires_at = now + datetime.timedelta(seconds=int(expires_in) - 60)  # Subtract a minute to be safe
        _token_cache[tenant_id] = {"access_token": access_token, "expires_at": expires_at}
        logger.debug(f"Token acquired for tenant {tenant_id}, expires at {expires_at.isoformat()}")
        return access_token
    else:
        error = result.get("error_description") or result.get("error")
        raise Exception(f"Failed to obtain token for tenant {tenant_id}: {error}")

def _make_request(url: str, headers: dict, params: dict, max_attempts: int = 3, base_delay: float = 1.0) -> dict:
    """
    Makes an HTTP GET request with retry logic for rate limiting and transient errors.
    
    Parameters:
        url (str): The URL for the GET request.
        headers (dict): HTTP headers to include.
        params (dict): Query parameters.
        max_attempts (int): Maximum number of attempts (default: 3).
        base_delay (float): Base delay in seconds for exponential backoff (default: 1.0).
    
    Returns:
        dict: The JSON response parsed as a dictionary.
    
    Raises:
        Exception: If all retry attempts fail.
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            response = requests.get(url, headers=headers, params=params)
            # If the response indicates rate limiting, raise an exception to retry.
            if response.status_code == 429:
                raise Exception("Rate limited (HTTP 429)")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Request to {url} failed on attempt {attempt + 1}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
    raise Exception(f"Failed to make request to {url} after {max_attempts} attempts.")

def _fetch_users(tenant_config: dict) -> list:
    """
    Synchronously fetches Azure AD users for a given tenant using Microsoft Graph API.
    
    Parameters:
        tenant_config (dict): Configuration for one tenant.
    
    Returns:
        list: A list of dictionaries representing Azure AD user objects.
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
    
    # Apply incremental discovery if "last_run" is provided (filter on lastModifiedDateTime)
    last_run = tenant_config.get("last_run")
    if last_run:
        params["$filter"] = f"lastModifiedDateTime gt {last_run}"
    
    users = []
    dedup_ids = set()
    url = base_url
    while url:
        try:
            data = _make_request(url, headers, params)
            for user in data.get("value", []):
                user_id = user.get("id")
                if user_id in dedup_ids:
                    continue
                dedup_ids.add(user_id)
                user["objectType"] = "user"
                user["source"] = "azure"
                users.append(user)
            url = data.get("@odata.nextLink")
            # NextLink URL already includes necessary query parameters.
            params = {}
        except Exception as e:
            logger.error(f"Error fetching Azure AD users for tenant {tenant_config.get('tenant_id')}: {e}")
            break
    return users

def _fetch_roles(tenant_config: dict) -> list:
    """
    Synchronously fetches Azure AD directory roles for a given tenant using Microsoft Graph API.
    
    Parameters:
        tenant_config (dict): Configuration for one tenant.
    
    Returns:
        list: A list of dictionaries representing Azure AD directory roles.
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
            data = _make_request(url, headers, params)
            for role in data.get("value", []):
                role_id = role.get("id")
                if role_id in dedup_ids:
                    continue
                dedup_ids.add(role_id)
                role["objectType"] = "role"
                role["source"] = "azure"
                roles.append(role)
            url = data.get("@odata.nextLink")
            params = {}
        except Exception as e:
            logger.error(f"Error fetching Azure AD roles for tenant {tenant_config.get('tenant_id')}: {e}")
            break
    return roles

async def fetch_identities(config: dict) -> list:
    """
    Asynchronously fetches Azure AD identities (users and directory roles) from one or more tenant configurations.
    Uses a dynamic ThreadPoolExecutor for concurrent processing.
    
    Parameters:
        config (dict): Configuration dictionary for Azure AD. It should contain either:
            - "tenants": a list of tenant configurations, or
            - Single tenant configuration parameters.
            Additional keys include "page_size", "last_run", and "max_workers".
    
    Returns:
        list: A combined list of dictionaries representing Azure AD users and roles.
    """
    logger.info("Starting Azure AD discovery asynchronously.")
    loop = asyncio.get_running_loop()
    max_workers = config.get("max_workers", 10)
    
    tenants = config.get("tenants")
    if not tenants:
        tenants = [config]
    
    tasks = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    
    for tenant_config in tenants:
        tasks.append(loop.run_in_executor(executor, _fetch_users, tenant_config))
        tasks.append(loop.run_in_executor(executor, _fetch_roles, tenant_config))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_identities = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error in Azure AD discovery task: {result}")
        else:
            all_identities.extend(result)
    
    logger.info(f"Fetched {len(all_identities)} Azure AD identity records from tenants.")
    return all_identities

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for Azure AD discovery supporting multiple tenants.
    test_config = {
        "tenants": [
            {
                "tenant_id": "your-tenant-id-1",
                "client_id": "your-client-id-1",
                "client_secret": "your-client-secret-1",
                "page_size": 100,
                "last_run": "2023-01-01T00:00:00Z"
            },
            {
                "tenant_id": "your-tenant-id-2",
                "client_id": "your-client-id-2",
                "client_secret": "your-client-secret-2",
                "page_size": 100,
                "last_run": "2023-01-01T00:00:00Z"
            }
        ],
        "max_workers": 10
    }
    
    asyncio.run(fetch_identities(test_config))
