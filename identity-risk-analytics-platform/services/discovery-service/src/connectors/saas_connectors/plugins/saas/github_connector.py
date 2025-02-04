#!/usr/bin/env python3
"""
github_connector.py

This module implements the GitHub connector plugin for the Discovery Service of the
AI-Powered Identity Risk Analytics Platform. It inherits from GenericSaasConnector and
retrieves identity and privilege data from GitHub using the GitHub API with asynchronous HTTP calls.

Features:
  - Fetches organization members (identities) from GitHub.
  - Fetches privilege data (e.g., OAuth installations) from GitHub.
  - Handles pagination and rate limiting via asynchronous HTTP requests.
  - Deduplicates records to avoid processing duplicates.
  - Implements a synchronous fetch_identities() wrapper for compatibility with our generic interface.

Security:
  - Authenticates via a personal access token provided in the configuration.
  - Communicates with GitHub over HTTPS.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import aiohttp

# Import the generic base class (ensure the PYTHONPATH is set appropriately)
from generic_saas_connector import GenericSaasConnector

logger = logging.getLogger("GitHubConnector")

class GitHubConnector(GenericSaasConnector):
    def initialize(self) -> None:
        """
        Perform any connector-specific initialization.
        Loads key parameters from configuration.
        """
        self.github_token = self.config.get("github_token")
        self.organization = self.config.get("organization")
        self.api_endpoint = self.config.get("api_endpoint", "https://api.github.com/")
        self.max_workers = self.config.get("max_workers", 10)
        self.api_timeout = self.config.get("api_timeout", 10)
        self.rate_limit_retry = self.config.get("rate_limit_retry", {"max_attempts": 5, "base_delay": 1.0})
        # Set Kafka topics if provided (for reference; publishing is handled elsewhere)
        self.kafka_topics = self.config.get("kafka_topics", {"identity": "github-identity", "privilege": "github-privilege"})
        # Initialize an in-memory cache for deduplication.
        self.cache = set()
        self.lock = asyncio.Lock()
        self.logger.info("GitHubConnector initialized.")

    async def handle_rate_limit(self, response: aiohttp.ClientResponse) -> None:
        """
        Handles GitHub API rate limiting by sleeping until the reset time.
        
        Parameters:
            response (aiohttp.ClientResponse): The response object.
        """
        if response.status in (403, 429):
            reset = response.headers.get("X-RateLimit-Reset")
            if reset:
                reset_time = int(reset)
                sleep_duration = max(reset_time - int(datetime.utcnow().timestamp()), 1)
                self.logger.warning(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
                await asyncio.sleep(sleep_duration)
            else:
                self.logger.error("Rate limit exceeded without reset header.")
                raise Exception("Rate limit exceeded without reset header.")

    async def fetch_paginated_data(self, url: str) -> List[Dict[str, Any]]:
        """
        Fetches paginated data from the given GitHub API URL.
        
        Parameters:
            url (str): The initial URL to fetch.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of JSON-decoded data.
        """
        data = []
        async with aiohttp.ClientSession(headers={
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }, timeout=aiohttp.ClientTimeout(total=self.api_timeout)) as session:
            while url:
                async with session.get(url) as response:
                    if response.status in (403, 429):
                        await self.handle_rate_limit(response)
                        continue
                    response.raise_for_status()
                    page_data = await response.json()
                    data.extend(page_data if isinstance(page_data, list) else page_data.get("value", []))
                    # GitHub uses Link header for pagination.
                    links = response.links
                    url = links.get('next', {}).get('url') if 'next' in links else None
        return data

    async def _async_fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches identity data (organization members) from GitHub.
        
        Returns:
            List[Dict[str, Any]]: List of identity records.
        """
        url = f"{self.api_endpoint}orgs/{self.organization}/members"
        members = await self.fetch_paginated_data(url)
        identities = []
        for member in members:
            record_id = str(member.get("id"))
            async with self.lock:
                if record_id in self.cache:
                    continue
                self.cache.add(record_id)
            identity = {
                "Username": member.get("login"),
                "UserID": record_id,
                "ProfileURL": member.get("html_url"),
                "LastActivity": member.get("updated_at")  # if provided by the API
            }
            identities.append(identity)
        self.logger.info(f"Fetched {len(identities)} identities from GitHub organization {self.organization}.")
        return identities

    def fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper to fetch identities.
        This method blocks until asynchronous identity retrieval is complete.
        
        Returns:
            List[Dict[str, Any]]: List of identity records.
        """
        return asyncio.run(self._async_fetch_identities())

    def fetch_privileges(self) -> List[Dict[str, Any]]:
        """
        Fetches privilege data from GitHub, such as OAuth app installations.
        This is implemented as a synchronous method using asyncio.
        
        Returns:
            List[Dict[str, Any]]: List of privilege records.
        """
        return asyncio.run(self._async_fetch_privileges())
    
    async def _async_fetch_privileges(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches privilege data from GitHub.
        For example, it fetches installations for the organization.
        
        Returns:
            List[Dict[str, Any]]: List of privilege records.
        """
        url = f"{self.api_endpoint}orgs/{self.organization}/installations"
        installations = await self.fetch_paginated_data(url)
        privileges = []
        for inst in installations:
            privilege = {
                "InstallationID": inst.get("id"),
                "AppSlug": inst.get("app_slug"),
                "TargetType": inst.get("target_type"),
                "TargetID": inst.get("target_id"),
                "Permissions": inst.get("permissions"),
                "CreatedAt": inst.get("created_at"),
                "UpdatedAt": inst.get("updated_at")
            }
            privilege["objectType"] = "privilege"
            privilege["source"] = "github"
            privileges.append(privilege)
        self.logger.info(f"Fetched {len(privileges)} privilege records from GitHub organization {self.organization}.")
        return privileges

    # Optionally, additional methods such as fetch_repositories, fetch_audit_logs, etc., could be added here,
    # but for the purpose of identity and privilege discovery, we implement only these two.

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Dummy configuration can be loaded from a YAML file in practice.
    dummy_config = {
        "github_token": "your_github_personal_access_token_here",
        "organization": "your_organization_name_here",
        "api_endpoint": "https://api.github.com/",
        "max_workers": 10,
        "api_timeout": 10,
        "rate_limit_retry": {
            "max_attempts": 5,
            "base_delay": 1.0
        },
        "kafka_topics": {
            "identity": "github-identity",
            "privilege": "github-privilege"
        }
    }
    
    connector = GitHubConnector(dummy_config)
    
    # Fetch identities synchronously.
    identities = connector.fetch_identities()
    print("Identities:")
    print(identities)
    
    # Fetch privileges synchronously.
    privileges = connector.fetch_privileges()
    print("Privileges:")
    print(privileges)
