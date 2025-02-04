#!/usr/bin/env python3
"""
gcp_connector.py

This module implements the GCP IAM connector for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform. It retrieves identity data from GCP by listing service accounts
and custom roles for one or more projects. The connector supports multiple projects, deduplicates records,
and uses asynchronous processing with dynamic thread pooling to scale in large environments.

Credentials are handled via the Google API Client (using ADC or environment-provided credentials), and all
communications are secured via HTTPS.

Note: GCP service account listings do not typically include a timestamp for incremental discovery; hence, this
connector fetches all available records for the given projects.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import json
from concurrent.futures import ThreadPoolExecutor
import datetime

from googleapiclient import discovery
from googleapiclient.errors import HttpError

logger = logging.getLogger("GCPConnector")

def _fetch_service_accounts(project_id: str, config: dict) -> list:
    """
    Synchronously fetches GCP service accounts for a given project using the Google IAM API.

    Parameters:
        project_id (str): The GCP project ID.
        config (dict): Configuration dictionary (currently used for potential future filters or max_workers).

    Returns:
        list: A list of dictionaries representing service account objects.
    """
    service = discovery.build('iam', 'v1')
    name = f"projects/{project_id}"
    users = []
    dedup_ids = set()

    try:
        request = service.projects().serviceAccounts().list(name=name)
        while request is not None:
            response = request.execute()
            accounts = response.get("accounts", [])
            for account in accounts:
                unique_id = account.get("uniqueId")
                if unique_id in dedup_ids:
                    continue
                dedup_ids.add(unique_id)
                account["objectType"] = "service_account"
                account["source"] = "gcp"
                # Convert any datetime fields if present (GCP API returns ISO strings already)
                users.append(account)
            request = service.projects().serviceAccounts().list_next(previous_request=request, previous_response=response)
    except HttpError as e:
        logger.error(f"Error fetching service accounts for project {project_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching service accounts for project {project_id}: {e}")
    
    return users

def _fetch_roles(project_id: str, config: dict) -> list:
    """
    Synchronously fetches custom IAM roles for a given project using the Google IAM API.
    Note: Google Cloud provides predefined roles as global resources; this function focuses on custom roles.

    Parameters:
        project_id (str): The GCP project ID.
        config (dict): Configuration dictionary.

    Returns:
        list: A list of dictionaries representing custom IAM roles.
    """
    service = discovery.build('iam', 'v1')
    parent = f"projects/{project_id}"
    roles = []
    dedup_ids = set()
    
    try:
        # List custom roles for the project
        request = service.projects().roles().list(parent=parent)
        while request is not None:
            response = request.execute()
            role_list = response.get("roles", [])
            for role in role_list:
                # Deduplicate based on role name
                role_name = role.get("name")
                if role_name in dedup_ids:
                    continue
                dedup_ids.add(role_name)
                role["objectType"] = "role"
                role["source"] = "gcp"
                # Optionally, convert createTime if present
                if "createTime" in role:
                    role["createTime"] = role["createTime"]
                roles.append(role)
            request = service.projects().roles().list_next(previous_request=request, previous_response=response)
    except HttpError as e:
        logger.error(f"Error fetching roles for project {project_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching roles for project {project_id}: {e}")
    
    return roles

async def fetch_identities(config: dict) -> list:
    """
    Asynchronously fetches GCP IAM identities from one or more projects.
    It retrieves both service accounts and custom roles, deduplicates records, and returns a combined list.
    
    Parameters:
        config (dict): Configuration dictionary that should contain either:
            - "projects": a list of project IDs to query, or
            - "project_id": a single project ID.
            - "max_workers": (Optional) Number of threads for concurrent processing (default: 10)
    
    Returns:
        list: A combined list of dictionaries representing service accounts and roles.
    """
    logger.info("Starting GCP IAM discovery asynchronously.")
    loop = asyncio.get_running_loop()
    max_workers = config.get("max_workers", 10)
    
    projects = config.get("projects")
    if not projects:
        projects = [config.get("project_id", "your-default-project-id")]
    
    tasks = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    
    for project_id in projects:
        tasks.append(loop.run_in_executor(executor, _fetch_service_accounts, project_id, config))
        tasks.append(loop.run_in_executor(executor, _fetch_roles, project_id, config))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_identities = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error in GCP IAM discovery task: {result}")
        else:
            all_identities.extend(result)
    
    logger.info(f"Fetched {len(all_identities)} IAM identity records from projects: {projects}")
    return all_identities

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for GCP IAM discovery supporting multiple projects.
    test_config = {
        "projects": ["your-project-id-1", "your-project-id-2"],
        "max_workers": 10
        # "last_run": "2023-01-01T00:00:00Z"  # Not applicable for service accounts as they lack a timestamp
    }
    
    asyncio.run(fetch_identities(test_config))
