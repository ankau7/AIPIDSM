#!/usr/bin/env python3
"""
linuxunix_resource_connector.py

This module implements the Linux/Unix Resource Discovery Connector for the
AI-Powered Identity Risk Analytics Platform. It connects to one or more Linux/Unix hosts via SSH
(using key-based authentication) and retrieves resource information by executing system commands.

Resource discovery in this connector includes:
  - NFS Exports: Extracted from /etc/exports.
  - SMB Shares: Extracted from /etc/samba/smb.conf.
  - Printers: Retrieved via the 'lpstat -p' command.

The connector parses the output of these commands, merges results into standardized resource records,
deduplicates entries (based on host, resource type, and resource name), and tags each record with:
  - "objectType": "resource"
  - "source": "linuxunix"
  - The originating host

SSH key–based authentication is used to avoid embedding plain-text passwords.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import asyncssh
import logging
import re
from typing import List, Dict, Any

logger = logging.getLogger("LinuxUnixResourceConnector")
logger.setLevel(logging.DEBUG)

class LinuxUnixResourceConnector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the Linux/Unix Resource Discovery Connector with the provided configuration.
        
        Expected configuration keys:
          - hosts: List of hostnames/IP addresses to scan.
          - ssh_username: SSH username for authentication.
          - ssh_private_key_path: Path to the SSH private key file for authentication.
            (Alternatively, "ssh_private_key" can be provided with the key's contents.)
          - resource_commands: (Optional) Mapping of resource types to commands. Defaults are provided.
              For example:
                nfs: "cat /etc/exports"
                smb: "cat /etc/samba/smb.conf"
                printers: "lpstat -p"
          - privilege_groups: (Optional) Not used here, but may be used in identity scanning.
          - max_workers: Maximum number of concurrent hosts to scan.
          - api_timeout: Timeout (in seconds) for SSH commands.
          - kafka_topics: (Optional) Kafka topic settings for publishing discovered resources.
        """
        self.config = config
        self.hosts = config.get("hosts", [])
        self.ssh_username = config.get("ssh_username")
        
        # Retrieve SSH private key either from key contents or key file.
        self.ssh_private_key = None
        if "ssh_private_key" in config:
            self.ssh_private_key = config["ssh_private_key"]
        elif "ssh_private_key_path" in config:
            key_path = config["ssh_private_key_path"]
            try:
                with open(key_path, "r") as f:
                    self.ssh_private_key = f.read()
            except Exception as e:
                logger.error("Failed to read SSH private key from %s: %s", key_path, e)
                raise
        else:
            raise ValueError("Configuration must include either 'ssh_private_key' or 'ssh_private_key_path'.")

        # Resource commands for each resource type; default commands are provided.
        self.resource_commands = config.get("resource_commands", {
            "nfs": "cat /etc/exports",
            "smb": "cat /etc/samba/smb.conf",
            "printers": "lpstat -p"
        })
        self.max_workers = config.get("max_workers", 10)
        self.api_timeout = config.get("api_timeout", 10)
        self.kafka_topics = config.get("kafka_topics", {"identity": "linuxunix-identity"})
        
        # Deduplication cache: key = "host:resource_type:resource_name"
        self.cache = set()
        self.lock = asyncio.Lock()
        
        logger.info("LinuxUnixResourceConnector initialized for hosts: %s", self.hosts)

    async def fetch_host_resources(self, host: str) -> List[Dict[str, Any]]:
        """
        Connects to a single host via SSH using key-based authentication and retrieves resource data.
        
        For each resource type defined in self.resource_commands, the connector executes the command and parses the output.
        
        Returns:
            List[Dict[str, Any]]: A list of resource records for the given host.
        """
        resources = []
        try:
            async with asyncssh.connect(host,
                                        username=self.ssh_username,
                                        client_keys=[self.ssh_private_key],
                                        known_hosts=None,
                                        timeout=self.api_timeout) as conn:
                for rtype, command in self.resource_commands.items():
                    try:
                        result = await conn.run(command, check=True)
                        output = result.stdout
                        parsed = self.parse_output(rtype, output)
                        # For each parsed resource, add host and tag data.
                        for rec in parsed:
                            rec["host"] = host
                            rec["objectType"] = "resource"
                            rec["source"] = "linuxunix"
                            rec["ResourceType"] = rtype  # e.g., "nfs", "smb", "printers"
                            # Use a deduplication key: host + resource type + resource name.
                            unique_key = f"{host}:{rtype}:{rec.get('ResourceName')}"
                            async with self.lock:
                                if unique_key in self.cache:
                                    continue
                                self.cache.add(unique_key)
                            resources.append(rec)
                    except Exception as ce:
                        logger.error("Error executing command for resource type '%s' on host %s: %s", rtype, host, ce)
        except Exception as e:
            logger.error("Error connecting to host %s: %s", host, e)
        return resources

    def parse_output(self, resource_type: str, output: str) -> List[Dict[str, Any]]:
        """
        Parses the command output for the given resource type into a list of resource records.
        
        Parameters:
            resource_type (str): The resource type (e.g., "nfs", "smb", "printers").
            output (str): The output of the command.
        
        Returns:
            List[Dict[str, Any]]: A list of resource records.
        """
        if resource_type == "nfs":
            return self.parse_nfs_exports(output)
        elif resource_type == "smb":
            return self.parse_smb_conf(output)
        elif resource_type == "printers":
            return self.parse_printers(output)
        else:
            logger.warning("Unknown resource type '%s'.", resource_type)
            return []

    def parse_nfs_exports(self, data: str) -> List[Dict[str, Any]]:
        """
        Parses the /etc/exports file to extract NFS export information.
        
        Expected format (each non-comment, non-empty line):
          /export/path client_options
          
        Returns:
            List[Dict[str, Any]]: List of resource records for NFS exports.
        """
        resources = []
        lines = data.strip().splitlines()
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Split line into tokens; first token is the exported directory, rest is client options.
            parts = line.split()
            if parts:
                export_path = parts[0]
                description = line  # You could store the entire line as a description.
                resource = {
                    "ResourceID": export_path,
                    "ResourceName": export_path,
                    "Description": description,
                    "CreatedDate": None,
                    "LastModifiedDate": None
                }
                resources.append(resource)
        return resources

    def parse_smb_conf(self, data: str) -> List[Dict[str, Any]]:
        """
        Parses the /etc/samba/smb.conf file to extract SMB share definitions.
        
        A simple parser that looks for share sections defined as:
          [sharename]
            path = /some/path
            comment = Some description
          
        Returns:
            List[Dict[str, Any]]: List of resource records for SMB shares.
        """
        resources = []
        current_share = None
        lines = data.strip().splitlines()
        for line in lines:
            line = line.strip()
            if line.startswith(";") or not line:
                continue
            # Check for a section header.
            if line.startswith("[") and line.endswith("]"):
                if current_share:
                    resources.append(current_share)
                share_name = line[1:-1].strip()
                current_share = {"ResourceID": share_name, "ResourceName": share_name, "Description": ""}
            elif "=" in line and current_share is not None:
                key, value = line.split("=", 1)
                key = key.strip().lower()
                value = value.strip()
                if key == "path":
                    current_share["Description"] += f"Path: {value}; "
                elif key == "comment":
                    current_share["Description"] += f"Comment: {value}; "
        if current_share:
            resources.append(current_share)
        return resources

    def parse_printers(self, data: str) -> List[Dict[str, Any]]:
        """
        Parses the output of 'lpstat -p' to extract printer information.
        
        Expected format (example):
          printer PrinterName is idle.  enabled since Thu Jan  1 00:00:00 1970
          
        Returns:
            List[Dict[str, Any]]: List of resource records for printers.
        """
        resources = []
        lines = data.strip().splitlines()
        for line in lines:
            line = line.strip()
            if line.lower().startswith("printer"):
                # Example: "printer PrinterName is idle.  enabled since Thu Jan  1 00:00:00 1970"
                parts = line.split()
                if len(parts) >= 2:
                    printer_name = parts[1]
                    resource = {
                        "ResourceID": printer_name,
                        "ResourceName": printer_name,
                        "Description": line,
                        "CreatedDate": None,
                        "LastModifiedDate": None
                    }
                    resources.append(resource)
        return resources

    async def _async_fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches resource records from all configured Linux/Unix hosts.
        
        Returns:
            List[Dict[str, Any]]: Aggregated list of resource records.
        """
        tasks = [self.fetch_host_resources(host) for host in self.hosts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_resources = []
        for result in results:
            if isinstance(result, Exception):
                logger.error("Error in fetching host resources: %s", result)
            else:
                all_resources.extend(result)
        logger.info("Fetched a total of %d resource records from Linux/Unix hosts.", len(all_resources))
        return all_resources

    def fetch_resources(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper to fetch resource records.
        
        Returns:
            List[Dict[str, Any]]: List of resource records.
        """
        return asyncio.run(self._async_fetch_resources())

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for Linux/Unix resource discovery using SSH key-based authentication.
    test_config = {
        "hosts": ["192.168.1.100", "192.168.1.101"],
        "ssh_username": "your_ssh_username",
        "ssh_private_key_path": "/path/to/your/private_key",
        "privilege_groups": ["sudo", "wheel"],
        "max_workers": 10,
        "api_timeout": 10,
        "last_run": "2023-01-01T00:00:00Z",
        "kafka_topics": {
            "identity": "linuxunix-identity"  # For resources, you might use "linuxunix-resource"
        },
        "resource_commands": {
            "nfs": "cat /etc/exports",
            "smb": "cat /etc/samba/smb.conf",
            "printers": "lpstat -p"
        }
    }
    
    connector = LinuxUnixResourceConnector(test_config)
    resources = connector.fetch_resources()
    print("Linux/Unix Resource Records:")
    print(resources)
