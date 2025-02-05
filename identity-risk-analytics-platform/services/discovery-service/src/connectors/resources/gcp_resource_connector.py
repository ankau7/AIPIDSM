#!/usr/bin/env python3
"""
gcp_resource_connector.py

This module implements the GCP Resource Discovery Connector for the AI-Powered Identity Risk Analytics Platform.
It uses the Cloud Asset Inventory API (via the google-cloud-asset library) to retrieve resource information
from one or more GCP projects.

Features:
  - Supports multi-project discovery by iterating over a list of GCP project IDs.
  - Retrieves assets with optional filtering by asset types.
  - Supports pagination via the Cloud Asset Inventory API.
  - Optionally applies an incremental snapshot filter using a read_time parameter if a last_run timestamp is provided.
  - Deduplicates resources based on their unique asset name.
  - Maps each asset into a standardized record with fields such as ResourceID, ResourceName, ResourceType, Location, and Tags.
  - Tags each record with "objectType": "resource" and "source": "gcp".
  - Uses asynchronous processing with a ThreadPoolExecutor to scan projects concurrently.
  - Relies on Application Default Credentials (ADC) for authentication (no credentials are embedded in code).

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from google.cloud import asset_v1
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger("GCPResourceConnector")
logger.setLevel(logging.DEBUG)

class GCPResourceConnector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the GCP Resource Connector with the provided configuration.
        
        Expected configuration keys:
          - projects: A list of GCP project IDs (e.g., ["project1", "project2"]). If not provided, a single "project_id" may be used.
          - asset_types: (Optional) A list of asset types to filter by (e.g., ["compute.googleapis.com/Instance", "storage.googleapis.com/Bucket"]).
          - page_size: Number of assets per page (default: 100).
          - last_run: (Optional) An ISO 8601 timestamp string to use as the read_time for incremental discovery.
          - max_workers: Maximum number of concurrent workers for scanning (default: 10).
          - kafka_topics: (Optional) Mapping for Kafka topics (e.g., {"resource": "gcp-resource"}).
        
        Authentication is handled via Application Default Credentials (ADC).
        """
        self.config = config
        self.projects = config.get("projects") or [config.get("project_id")]
        self.asset_types = config.get("asset_types", [])
        self.page_size = config.get("page_size", 100)
        self.last_run = config.get("last_run")
        self.max_workers = config.get("max_workers", 10)
        self.kafka_topics = config.get("kafka_topics", {"resource": "gcp-resource"})
        
        # Deduplication cache: use a set keyed by asset name (ARN).
        self.cache = set()
        
        # Initialize the Cloud Asset Inventory client.
        self.client = asset_v1.AssetServiceClient()
        
        logger.info("GCPResourceConnector initialized for projects: %s with page_size=%d.", self.projects, self.page_size)

    def _convert_timestamp(self, iso_str: str) -> Timestamp:
        """
        Converts an ISO 8601 string into a google.protobuf.timestamp_pb2.Timestamp.
        
        Parameters:
            iso_str (str): ISO 8601 timestamp string.
        
        Returns:
            Timestamp: The corresponding protobuf Timestamp.
        """
        dt = datetime.fromisoformat(iso_str)
        ts = Timestamp()
        ts.FromDatetime(dt)
        return ts

    def _fetch_resources_for_project(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Synchronously fetches resource assets for a given GCP project using the Cloud Asset Inventory API.
        
        Parameters:
            project_id (str): The GCP project ID.
        
        Returns:
            List[Dict[str, Any]]: A list of standardized resource records for the project.
        """
        resources = []
        parent = f"projects/{project_id}"
        request = asset_v1.ListAssetsRequest(
            parent=parent,
            page_size=self.page_size,
            content_type=asset_v1.ContentType.RESOURCE
        )
        
        if self.asset_types:
            request.asset_types = self.asset_types
        
        if self.last_run:
            request.read_time = self._convert_timestamp(self.last_run)
        else:
            ts = Timestamp()
            ts.GetCurrentTime()
            request.read_time = ts
        
        try:
            page_result = self.client.list_assets(request=request)
            for asset in page_result:
                asset_name = asset.name  # Unique identifier, e.g., the asset ARN.
                if asset_name in self.cache:
                    continue
                self.cache.add(asset_name)
                
                # Convert asset.resource.data (if available) to a dict.
                resource_data = {}
                if asset.resource and asset.resource.data:
                    resource_data = asset.resource.data
                
                # Attempt to extract a friendly name from resource_data.
                resource_name = resource_data.get("name") if "name" in resource_data else asset_name
                
                # Determine resource type from asset.asset_type.
                resource_type = asset.asset_type  # e.g., "compute.googleapis.com/Instance"
                
                # Optionally extract location.
                location = resource_data.get("location") if resource_data else None
                
                resource_record = {
                    "ResourceID": asset_name,
                    "ResourceName": resource_name,
                    "ResourceType": resource_type,
                    "Location": location,
                    "Tags": resource_data.get("labels") if resource_data and "labels" in resource_data else {},
                    "CreatedDate": None,       # Not provided via this API.
                    "LastModifiedDate": None,  # Not provided via this API.
                    "objectType": "resource",
                    "source": "gcp",
                    "project": project_id
                }
                resources.append(resource_record)
        except Exception as e:
            logger.error("Error fetching resources for project %s: %s", project_id, e)
        
        return resources

    async def _async_fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches resource records from all configured GCP projects using a ThreadPoolExecutor.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of resource records from all projects.
        """
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        tasks = [
            loop.run_in_executor(executor, self._fetch_resources_for_project, project)
            for project in self.projects
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_resources = []
        for result in results:
            if isinstance(result, Exception):
                logger.error("Error in GCP resource fetching task: %s", result)
            else:
                all_resources.extend(result)
        logger.info("Fetched a total of %d resource records from projects: %s", len(all_resources), self.projects)
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
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for GCP resource discovery with multi-project support.
    test_config = {
        "projects": ["your-project-id-1", "your-project-id-2"],
        "asset_types": ["compute.googleapis.com/Instance", "storage.googleapis.com/Bucket"],
        "page_size": 100,
        # "last_run": "2023-01-01T00:00:00Z",  # Optional: for incremental discovery.
        "max_workers": 10,
        "kafka_topics": {
            "resource": "gcp-resource"
        }
    }
    
    connector = GCPResourceConnector(test_config)
    resources = connector.fetch_resources()
    print("GCP Resource Records:")
    print(resources)
