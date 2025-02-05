#!/usr/bin/env python3
"""
aws_resource_connector.py

This module implements the AWS Resource Discovery Connector for the AI-Powered Identity Risk Analytics Platform.
It retrieves resource information from AWS using the Resource Groups Tagging API. This connector:
  - Supports multi-region discovery by iterating over a list of AWS regions.
  - Uses the Resource Groups Tagging API to fetch tagged resources.
  - Handles pagination via the PaginationToken.
  - Deduplicates resources based on their unique ARN.
  - Maps each resource into a standardized record with fields such as ResourceID, ResourceName, ResourceType, Tags, etc.
  - Uses asynchronous processing with ThreadPoolExecutor to run region scans concurrently.
  - Relies on AWS credentials provided via boto3’s best practices (IAM roles, environment variables, etc.).

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import logging
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("AWSResourceConnector")
logger.setLevel(logging.DEBUG)

class AWSResourceConnector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the AWS Resource Connector.
        
        Expected configuration keys:
          - regions: (Optional) List of AWS regions (e.g., ["us-east-1", "us-west-2"]). If omitted, uses the value of "region".
          - region: (Optional) Single AWS region to use if "regions" is not provided.
          - page_size: Number of resources per page (default: 50).
          - max_workers: Maximum number of concurrent workers (default: 10).
          - kafka_topics: (Optional) Kafka topic mapping (e.g., {"resource": "aws-resource"}).
        
        AWS credentials are managed via boto3’s standard methods.
        """
        self.config = config
        # Use "regions" if provided; otherwise, use the single "region" (default "us-east-1")
        self.regions = config.get("regions") or [config.get("region", "us-east-1")]
        self.page_size = config.get("page_size", 50)
        self.max_workers = config.get("max_workers", 10)
        self.kafka_topics = config.get("kafka_topics", {"resource": "aws-resource"})
        
        # Deduplication: use a set of resource ARNs.
        self.cache = set()
        
        # Initialize the boto3 client later on per region.
        logger.info("AWSResourceConnector initialized for regions: %s with page_size=%d.", self.regions, self.page_size)

    def _fetch_resources_for_region(self, region: str) -> List[Dict[str, Any]]:
        """
        Synchronously fetches resources from a given AWS region using the Resource Groups Tagging API.
        
        Parameters:
            region (str): AWS region name.
        
        Returns:
            List[Dict[str, Any]]: List of standardized resource records from that region.
        """
        resources = []
        client = boto3.client("resourcegroupstaggingapi", region_name=region)
        pagination_token = None
        
        try:
            while True:
                kwargs = {"ResourcesPerPage": self.page_size}
                if pagination_token:
                    kwargs["PaginationToken"] = pagination_token
                response = client.get_resources(**kwargs)
                resource_list = response.get("ResourceTagMappingList", [])
                for mapping in resource_list:
                    resource_arn = mapping.get("ResourceARN")
                    if not resource_arn:
                        continue
                    # Deduplicate based on ARN.
                    if resource_arn in self.cache:
                        continue
                    self.cache.add(resource_arn)
                    
                    # Extract a friendly name from tags if available.
                    tags = mapping.get("Tags", [])
                    resource_name = None
                    for tag in tags:
                        if tag.get("Key", "").lower() == "name":
                            resource_name = tag.get("Value")
                            break
                    
                    # Parse the ARN to determine resource type.
                    # ARN format: arn:partition:service:region:account-id:resource-type/resource-id
                    arn_parts = resource_arn.split(":")
                    resource_type = "unknown"
                    if len(arn_parts) >= 6:
                        # The 6th part might be in the format "resource-type/resource-id"
                        resource_info = arn_parts[5]
                        if "/" in resource_info:
                            resource_type = resource_info.split("/")[0]
                        else:
                            resource_type = resource_info
                    
                    resource_record = {
                        "ResourceID": resource_arn,
                        "ResourceName": resource_name if resource_name else resource_arn,
                        "ResourceType": resource_type,
                        "Tags": tags,
                        "Description": "",   # Not provided by the API.
                        "CreatedDate": None,  # Not available via this API.
                        "LastModifiedDate": None,  # Not available.
                        "objectType": "resource",
                        "source": "aws",
                        "region": region
                    }
                    resources.append(resource_record)
                
                pagination_token = response.get("PaginationToken")
                if not pagination_token:
                    break
        except ClientError as e:
            logger.error("Error fetching AWS resources in region %s: %s", region, e)
        except Exception as e:
            logger.error("Unexpected error in AWSResourceConnector for region %s: %s", region, e)
        
        return resources

    async def _async_fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches resource records from all configured AWS regions using a ThreadPoolExecutor.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of resource records from all regions.
        """
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        tasks = []
        for region in self.regions:
            tasks.append(loop.run_in_executor(executor, self._fetch_resources_for_region, region))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_resources = []
        for result in results:
            if isinstance(result, Exception):
                logger.error("Error in AWS resource fetching task: %s", result)
            else:
                all_resources.extend(result)
        logger.info("Fetched a total of %d resource records from regions: %s", len(all_resources), self.regions)
        return all_resources

    def fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper for asynchronous resource fetching.
        
        Returns:
            List[Dict[str, Any]]: List of resource records.
        """
        return asyncio.run(self._async_fetch_resources())

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for AWS resource discovery with multi-region support.
    test_config = {
        "regions": ["us-east-1", "us-west-2"],
        "page_size": 50,
        "max_workers": 10,
        "kafka_topics": {
            "resource": "aws-resource"
        }
    }
    
    connector = AWSResourceConnector(test_config)
    resources = connector.fetch_resources()
    print("AWS Resource Records:")
    print(resources)
