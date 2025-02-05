#!/usr/bin/env python3
"""
azure_resource_connector.py

This module implements the Azure Resource Discovery Connector for the AI-Powered Identity Risk Analytics Platform.
It uses the Azure Resource Graph API (via the azure-mgmt-resourcegraph client library) to retrieve resource information
from one or more Azure subscriptions.

Key Features:
  - Supports multi-subscription discovery by iterating over a list of subscription IDs.
  - Executes a configurable Kusto Query Language (KQL) query to retrieve resources.
  - Optionally applies an incremental snapshot filter using a read_time parameter if a last_run timestamp is provided.
  - Deduplicates resources based on their unique resource id.
  - Maps each asset into a standardized record with fields such as ResourceID, ResourceName, ResourceType, Location, and Tags.
  - Tags each record with "objectType": "resource" and "source": "azure", including the subscription ID.
  - Uses asynchronous processing with a ThreadPoolExecutor to scan subscriptions concurrently.
  - Relies on Application Default Credentials (ADC) for authentication (no credentials embedded in code).

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest
from google.protobuf.timestamp_pb2 import Timestamp  # For consistency in our connectors, if needed

logger = logging.getLogger("AzureResourceConnector")
logger.setLevel(logging.DEBUG)

class AzureResourceConnector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the Azure Resource Connector with the provided configuration.
        
        Expected configuration keys:
          - subscriptions: A list of Azure subscription IDs (e.g., ["sub1", "sub2"]). If not provided, a single "subscription_id" is used.
          - query: The KQL query to retrieve resources (default returns id, name, type, location, tags).
          - options: (Optional) Additional options for the query.
          - last_run: (Optional) An ISO 8601 timestamp string to use as the readTime for incremental discovery.
          - page_size: (Optional) Number of assets per page (default: 100). (Note: Resource Graph API handles paging internally.)
          - max_workers: Maximum number of concurrent workers (default: 10).
          - kafka_topics: (Optional) Mapping for Kafka topics (e.g., {"resource": "azure-resource"}).
        
        Authentication is handled via DefaultAzureCredential (ADC).
        """
        self.config = config
        self.subscriptions = config.get("subscriptions") or [config.get("subscription_id")]
        self.query = config.get("query", "Resources | project id, name, type, location, tags")
        self.options = config.get("options", {})
        self.last_run = config.get("last_run")  # Optional incremental discovery.
        self.page_size = config.get("page_size", 100)  # Not directly used by Resource Graph.
        self.max_workers = config.get("max_workers", 10)
        self.kafka_topics = config.get("kafka_topics", {"resource": "azure-resource"})
        
        # Deduplication: use a set keyed by the unique resource id.
        self.cache = set()
        
        # Set up the Resource Graph client using DefaultAzureCredential.
        self.credential = DefaultAzureCredential()
        self.client = ResourceGraphClient(self.credential)
        
        logger.info("AzureResourceConnector initialized for subscriptions: %s", self.subscriptions)

    def _build_query_request(self, subscription: str) -> QueryRequest:
        """
        Builds a QueryRequest object for the given subscription.
        
        If last_run is provided, it sets the readTime accordingly.
        
        Parameters:
            subscription (str): The Azure subscription ID.
        
        Returns:
            QueryRequest: The constructed query request.
        """
        parent = f"subscriptions/{subscription}"
        request = QueryRequest(
            parent=parent,
            query=self.query,
            options=self.options
        )
        if self.last_run:
            # Convert the ISO 8601 string to a Timestamp.
            dt = datetime.fromisoformat(self.last_run)
            ts = Timestamp()
            ts.FromDatetime(dt)
            request.read_time = ts
        else:
            # Use current time.
            ts = Timestamp()
            ts.GetCurrentTime()
            request.read_time = ts
        return request

    def _fetch_resources_for_subscription(self, subscription: str) -> List[Dict[str, Any]]:
        """
        Synchronously fetches resource assets for a given Azure subscription using the Resource Graph API.
        
        Parameters:
            subscription (str): The Azure subscription ID.
        
        Returns:
            List[Dict[str, Any]]: A list of standardized resource records for the subscription.
        """
        resources = []
        try:
            request = self._build_query_request(subscription)
            response = self.client.resources(request=request)
            # The response data is a pandas DataFrame.
            # Convert it to a list of dictionaries.
            records = response.data.to_dict(orient="records")
            for record in records:
                resource_id = record.get("id")
                if not resource_id:
                    continue
                if resource_id in self.cache:
                    continue
                self.cache.add(resource_id)
                resource_name = record.get("name") or resource_id
                resource_type = record.get("type")
                location = record.get("location")
                tags = record.get("tags") or {}
                mapped = {
                    "ResourceID": resource_id,
                    "ResourceName": resource_name,
                    "ResourceType": resource_type,
                    "Location": location,
                    "Tags": tags,
                    "CreatedDate": None,       # Not provided by Resource Graph.
                    "LastModifiedDate": None,  # Not provided.
                    "objectType": "resource",
                    "source": "azure",
                    "subscription": subscription
                }
                resources.append(mapped)
        except Exception as e:
            logger.error("Error fetching resources for subscription %s: %s", subscription, e)
        return resources

    async def _async_fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches resource records from all configured Azure subscriptions using a ThreadPoolExecutor.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of resource records.
        """
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        tasks = [
            loop.run_in_executor(executor, self._fetch_resources_for_subscription, subscription)
            for subscription in self.subscriptions
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_resources = []
        for result in results:
            if isinstance(result, Exception):
                logger.error("Error in Azure resource fetching task: %s", result)
            else:
                all_resources.extend(result)
        logger.info("Fetched a total of %d resource records from subscriptions: %s", len(all_resources), self.subscriptions)
        return all_resources

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
    
    # Example configuration for Azure resource discovery with multi-subscription support.
    test_config = {
        "subscriptions": ["your-subscription-id-1", "your-subscription-id-2"],
        "query": "Resources | project id, name, type, location, tags",
        # "last_run": "2023-01-01T00:00:00Z",  # Optional: For incremental discovery.
        "max_workers": 10,
        "kafka_topics": {"resource": "azure-resource"}
    }
    
    connector = AzureResourceConnector(test_config)
    resources = connector.fetch_resources()
    print("Azure Resource Records:")
    print(resources)
