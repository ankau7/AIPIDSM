#!/usr/bin/env python3
"""
ad_connector.py

This module implements the enhanced Active Directory (AD) connector for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform. It retrieves comprehensive identity data from AD using LDAPS,
supporting both user and computer objects—including local admin accounts if available—and ensures that
only new or modified records are retrieved via the "whenChanged" filter.

Key Features:
  - Retrieves user objects (e.g., sAMAccountName, displayName, mail, userAccountControl, whenCreated, whenChanged, objectGUID, distinguishedName, memberOf)
  - Retrieves computer objects (e.g., sAMAccountName, dNSHostName, operatingSystem, operatingSystemVersion, userAccountControl, whenCreated, whenChanged, objectGUID, distinguishedName)
  - Supports incremental discovery via a "last_run" timestamp
  - Implements deduplication using unique identifiers (objectGUID or sAMAccountName)
  - Supports multiple AD domains; each domain is processed concurrently using a dynamic ThreadPoolExecutor based on "max_workers"
  - Uses LDAP paging and a simple retry mechanism with exponential backoff for robust connectivity
  - Returns a combined list of records with an "objectType" field for downstream processing

Security:
  - Uses LDAPS (LDAP over SSL) for secure communication
  - Credentials and sensitive parameters are expected to be passed in the configuration securely (e.g., via environment variables)

Note: Scheduling (i.e., triggering discovery periodically or manually) is handled externally.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from ldap3 import Server, Connection, ALL, SUBTREE
from ldap3.core.exceptions import LDAPException

logger = logging.getLogger("ADConnector")

def _retry(func, max_attempts=3, base_delay=1, *args, **kwargs):
    """
    Retry helper with exponential backoff.
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
    raise Exception(f"Function {func.__name__} failed after {max_attempts} attempts")

def _fetch_users(domain_config: dict) -> list:
    """
    Synchronously fetches user objects from a single AD domain using LDAP paging and retry logic.

    Parameters:
        domain_config (dict): AD configuration for one domain.

    Returns:
        list: A list of dictionaries representing user objects.
    """
    server_url = domain_config["server"]
    user_dn = domain_config["user"]
    password = domain_config["password"]
    base_dn = domain_config["base_dn"]
    last_run = domain_config.get("last_run")
    page_size = domain_config.get("page_size", 1000)
    attributes = domain_config.get("attributes", [
        "sAMAccountName", "displayName", "mail", "userAccountControl",
        "whenCreated", "whenChanged", "objectGUID", "distinguishedName", "memberOf", "userPrincipalName", "lastLogonTimestamp"
    ])

    # Build LDAP filter for user objects; include whenChanged filter for incremental discovery if provided.
    filter_parts = ["(objectClass=user)"]
    if last_run:
        filter_parts.append(f"(whenChanged>={last_run})")
    search_filter = "(&" + "".join(filter_parts) + ")"

    users = []
    dedup_ids = set()  # For deduplication based on objectGUID or sAMAccountName.
    
    try:
        server = Server(server_url, use_ssl=True, get_info=ALL)
        conn = Connection(server, user=user_dn, password=password, auto_bind=True)
        cookie = None
        while True:
            _retry(conn.search, max_attempts=3, base_delay=1,
                   search_base=base_dn,
                   search_filter=search_filter,
                   search_scope=SUBTREE,
                   attributes=attributes,
                   paged_size=page_size,
                   paged_cookie=cookie)
            
            for entry in conn.entries:
                entry_json = entry.entry_to_json()
                entry_dict = json.loads(entry_json)
                # Use objectGUID if available; otherwise, fallback to sAMAccountName.
                unique_id = entry_dict.get("attributes", {}).get("objectGUID") or \
                            entry_dict.get("attributes", {}).get("sAMAccountName")
                if unique_id in dedup_ids:
                    continue
                dedup_ids.add(unique_id)
                entry_dict["objectType"] = "user"
                users.append(entry_dict)
            
            controls = conn.result.get("controls", {})
            page_control = controls.get("1.2.840.113556.1.4.319", {})
            cookie = page_control.get("value", {}).get("cookie")
            if not cookie:
                break
        conn.unbind()
    except LDAPException as e:
        logger.error(f"LDAP error fetching users from {server_url}: {e}")
    except Exception as e:
        logger.error(f"Error fetching users from {server_url}: {e}")
    
    return users

def _fetch_computers(domain_config: dict) -> list:
    """
    Synchronously fetches computer objects from a single AD domain using LDAP paging and retry logic.

    Parameters:
        domain_config (dict): AD configuration for one domain.

    Returns:
        list: A list of dictionaries representing computer objects.
    """
    server_url = domain_config["server"]
    user_dn = domain_config["user"]
    password = domain_config["password"]
    base_dn = domain_config["base_dn"]
    last_run = domain_config.get("last_run")
    page_size = domain_config.get("page_size", 1000)
    attributes = domain_config.get("computer_attributes", [
        "sAMAccountName", "dNSHostName", "operatingSystem", "operatingSystemVersion",
        "userAccountControl", "whenCreated", "whenChanged", "objectGUID", "distinguishedName"
    ])

    # Build LDAP filter for computer objects; include whenChanged filter for incremental discovery if provided.
    filter_parts = ["(objectClass=computer)"]
    if last_run:
        filter_parts.append(f"(whenChanged>={last_run})")
    search_filter = "(&" + "".join(filter_parts) + ")"

    computers = []
    dedup_ids = set()
    try:
        server = Server(server_url, use_ssl=True, get_info=ALL)
        conn = Connection(server, user=user_dn, password=password, auto_bind=True)
        cookie = None
        while True:
            _retry(conn.search, max_attempts=3, base_delay=1,
                   search_base=base_dn,
                   search_filter=search_filter,
                   search_scope=SUBTREE,
                   attributes=attributes,
                   paged_size=page_size,
                   paged_cookie=cookie)
            
            for entry in conn.entries:
                entry_json = entry.entry_to_json()
                entry_dict = json.loads(entry_json)
                unique_id = entry_dict.get("attributes", {}).get("objectGUID") or \
                            entry_dict.get("attributes", {}).get("sAMAccountName")
                if unique_id in dedup_ids:
                    continue
                dedup_ids.add(unique_id)
                entry_dict["objectType"] = "computer"
                computers.append(entry_dict)
            
            controls = conn.result.get("controls", {})
            page_control = controls.get("1.2.840.113556.1.4.319", {})
            cookie = page_control.get("value", {}).get("cookie")
            if not cookie:
                break
        conn.unbind()
    except LDAPException as e:
        logger.error(f"LDAP error fetching computers from {server_url}: {e}")
    except Exception as e:
        logger.error(f"Error fetching computers from {server_url}: {e}")
    return computers

async def fetch_identities(config: dict) -> list:
    """
    Asynchronously fetches identity data from Active Directory across one or more domain configurations.
    Uses a dynamic ThreadPoolExecutor (with max_workers specified per domain) to concurrently run blocking LDAP queries
    for both user and computer objects. Supports incremental discovery via the 'last_run' filter.
    
    Parameters:
        config (dict): Configuration dictionary. It can either be a single domain configuration or include a "domains"
                       key with a list of domain configurations.
    
    Returns:
        list: A combined list of dictionaries representing both user and computer objects with an "objectType" field.
    """
    logger.info("Starting asynchronous AD discovery.")
    
    # Determine domain configurations: if "domains" exists, use that list; otherwise, treat the config as a single domain.
    domains = config.get("domains")
    if domains is None:
        domains = [config]
    
    loop = asyncio.get_running_loop()
    all_identities = []
    tasks = []

    for domain_config in domains:
        max_workers = domain_config.get("max_workers", 10)
        executor = ThreadPoolExecutor(max_workers=max_workers)
        tasks.append(loop.run_in_executor(executor, _fetch_users, domain_config))
        tasks.append(loop.run_in_executor(executor, _fetch_computers, domain_config))
    
    # Wait for all tasks to complete concurrently.
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error in AD discovery task: {result}")
        else:
            all_identities.extend(result)
    
    logger.info(f"Fetched a total of {len(all_identities)} identity records from AD across domains.")
    return all_identities

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration with multiple domains and incremental discovery.
    test_config = {
        "domains": [
            {
                "server": "ldaps://ad-server1.example.com",
                "user": "CN=ServiceAccount,CN=Users,DC=example,DC=com",
                "password": "password1",
                "base_dn": "DC=example,DC=com",
                "last_run": "20230101000000Z",
                "page_size": 500,
                "attributes": [
                    "sAMAccountName", "displayName", "mail", "userAccountControl",
                    "whenCreated", "whenChanged", "objectGUID", "distinguishedName", "memberOf", "userPrincipalName", "lastLogonTimestamp"
                ],
                "computer_attributes": [
                    "sAMAccountName", "dNSHostName", "operatingSystem", "operatingSystemVersion",
                    "userAccountControl", "whenCreated", "whenChanged", "objectGUID", "distinguishedName"
                ],
                "max_workers": 10
            },
            {
                "server": "ldaps://ad-server2.example.com",
                "user": "CN=ServiceAccount,CN=Users,DC=other,DC=example,DC=com",
                "password": "password2",
                "base_dn": "DC=other,DC=example,DC=com",
                "last_run": "20230101000000Z",
                "page_size": 500,
                "attributes": [
                    "sAMAccountName", "displayName", "mail", "userAccountControl",
                    "whenCreated", "whenChanged", "objectGUID", "distinguishedName", "memberOf", "userPrincipalName", "lastLogonTimestamp"
                ],
                "computer_attributes": [
                    "sAMAccountName", "dNSHostName", "operatingSystem", "operatingSystemVersion",
                    "userAccountControl", "whenCreated", "whenChanged", "objectGUID", "distinguishedName"
                ],
                "max_workers": 10
            }
        ]
    }
    
    # Run the asynchronous fetch_identities function using the test configuration.
    asyncio.run(fetch_identities(test_config))
