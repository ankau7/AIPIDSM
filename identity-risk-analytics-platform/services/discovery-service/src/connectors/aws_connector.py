#!/usr/bin/env python3
"""
aws_connector.py

This module implements the AWS IAM connector for the Discovery Service of the AI-Powered Identity Risk Analytics Platform.
It retrieves identity data from AWS IAM, including both IAM users and IAM roles, and supports incremental discovery using a 
"last_run" filter. This version supports multiple regions dynamically by reading a list of regions from the configuration.
Secure connections are ensured by boto3 via HTTPS, with credentials managed securely via environment variables, AWS config, or IAM roles.
The connector uses asynchronous processing with a dynamic ThreadPoolExecutor to handle large environments efficiently.

Features:
    - Retrieves IAM users with attributes such as UserName, UserId, Arn, CreateDate, PasswordLastUsed, etc.
    - Retrieves IAM roles with attributes such as RoleName, RoleId, Arn, and CreateDate.
    - Supports incremental discovery using a "last_run" filter.
    - Supports multiple regions via a list in the configuration.
    - Deduplicates records based on unique identifiers.
    - Uses asynchronous processing and dynamic thread pooling for parallel processing.
    - Secure connection via boto3 (HTTPS), with credentials managed securely.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import json
import datetime
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("AWSConnector")

def parse_iso8601(dt_str: str) -> datetime.datetime:
    """
    Parses an ISO 8601 datetime string into a datetime object.
    If the string ends with 'Z', it is replaced with '+00:00'.
    
    Parameters:
        dt_str (str): The ISO 8601 datetime string.
    
    Returns:
        datetime.datetime: The parsed datetime object.
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(dt_str)

def _fetch_users(region: str, config: dict) -> list:
    """
    Synchronously fetches IAM users from AWS in the specified region using boto3's paginator.
    Applies incremental discovery by filtering based on the "last_run" timestamp.
    
    Parameters:
        region (str): AWS region name.
        config (dict): Configuration dictionary including:
            - last_run: (Optional) ISO 8601 datetime string to filter users created/updated after this time.
    
    Returns:
        list: A list of dictionaries representing IAM user objects.
    """
    client = boto3.client("iam", region_name=region)
    paginator = client.get_paginator("list_users")
    
    last_run_str = config.get("last_run")
    last_run = None
    if last_run_str:
        try:
            last_run = parse_iso8601(last_run_str)
        except Exception as e:
            logger.error(f"Error parsing last_run '{last_run_str}': {e}")
    
    users = []
    dedup_ids = set()
    
    try:
        for page in paginator.paginate():
            for user in page.get("Users", []):
                # Filter based on CreateDate if last_run is provided.
                create_date = user.get("CreateDate")
                if last_run and create_date < last_run:
                    continue
                user_id = user.get("UserId")
                if user_id in dedup_ids:
                    continue
                dedup_ids.add(user_id)
                user["objectType"] = "user"
                user["source"] = "aws"
                # Convert datetime objects to ISO strings
                if isinstance(create_date, datetime.datetime):
                    user["CreateDate"] = create_date.isoformat()
                if user.get("PasswordLastUsed") and isinstance(user.get("PasswordLastUsed"), datetime.datetime):
                    user["PasswordLastUsed"] = user["PasswordLastUsed"].isoformat()
                users.append(user)
    except ClientError as e:
        logger.error(f"Error fetching AWS IAM users in region {region}: {e}")
    return users

def _fetch_roles(region: str, config: dict) -> list:
    """
    Synchronously fetches IAM roles from AWS in the specified region using boto3's paginator.
    Applies incremental discovery by filtering based on the "last_run" timestamp.
    
    Parameters:
        region (str): AWS region name.
        config (dict): Configuration dictionary including:
            - last_run: (Optional) ISO 8601 datetime string to filter roles created after this time.
    
    Returns:
        list: A list of dictionaries representing IAM role objects.
    """
    client = boto3.client("iam", region_name=region)
    paginator = client.get_paginator("list_roles")
    
    last_run_str = config.get("last_run")
    last_run = None
    if last_run_str:
        try:
            last_run = parse_iso8601(last_run_str)
        except Exception as e:
            logger.error(f"Error parsing last_run '{last_run_str}': {e}")
    
    roles = []
    dedup_ids = set()
    
    try:
        for page in paginator.paginate():
            for role in page.get("Roles", []):
                create_date = role.get("CreateDate")
                if last_run and create_date < last_run:
                    continue
                role_id = role.get("RoleId")
                if role_id in dedup_ids:
                    continue
                dedup_ids.add(role_id)
                role["objectType"] = "role"
                role["source"] = "aws"
                if isinstance(create_date, datetime.datetime):
                    role["CreateDate"] = create_date.isoformat()
                roles.append(role)
    except ClientError as e:
        logger.error(f"Error fetching AWS IAM roles in region {region}: {e}")
    return roles

async def fetch_identities(config: dict) -> list:
    """
    Asynchronously fetches AWS IAM identities (users and roles) from multiple regions.
    If the configuration contains a "regions" key (a list), it iterates over each region; otherwise,
    it falls back to a single region using the "region" key (default "us-east-1").
    
    Parameters:
        config (dict): Configuration dictionary containing keys such as:
            - "regions": (Optional) List of AWS regions to query.
            - "region": (Optional) Single AWS region (default if "regions" not provided).
            - "last_run": (Optional) ISO 8601 datetime string for incremental discovery.
            - "max_workers": (Optional) Number of threads for concurrent processing (default: 10).
    
    Returns:
        list: A combined list of dictionaries representing IAM users and roles.
    """
    logger.info("Starting AWS IAM discovery asynchronously.")
    loop = asyncio.get_running_loop()
    max_workers = config.get("max_workers", 10)
    
    # Determine list of regions to query.
    regions = config.get("regions")
    if not regions:
        regions = [config.get("region", "us-east-1")]
    
    tasks = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    
    for region in regions:
        tasks.append(loop.run_in_executor(executor, _fetch_users, region, config))
        tasks.append(loop.run_in_executor(executor, _fetch_roles, region, config))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_identities = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error in AWS IAM discovery task: {result}")
        else:
            all_identities.extend(result)
    
    logger.info(f"Fetched {len(all_identities)} IAM identity records from regions: {regions}")
    return all_identities

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration supporting multiple regions.
    test_config = {
        "regions": ["us-east-1", "us-west-2"],
        "last_run": "2023-01-01T00:00:00Z",
        "max_workers": 10
    }
    asyncio.run(fetch_identities(test_config))
