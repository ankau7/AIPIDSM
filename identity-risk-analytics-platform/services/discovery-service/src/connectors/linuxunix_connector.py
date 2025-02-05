#!/usr/bin/env python3
"""
linuxunix_connector.py

This module implements the Linux/Unix discovery connector for the AI-Powered Identity Risk Analytics Platform.
It retrieves identity data from one or more Linux/Unix hosts by connecting via SSH using key-based authentication
(with asyncssh) and reading the /etc/passwd and /etc/group files. It then parses these files to build standardized identity records,
including group memberships. Users belonging to configured privilege groups (e.g., "sudo", "wheel") are flagged as administrators.

Key Features:
  - Asynchronously connects to multiple Linux/Unix hosts via SSH using a private key for authentication.
  - Reads and parses /etc/passwd for user account details.
  - Reads and parses /etc/group to capture group memberships.
  - Merges group membership data into each user record.
  - Flags users as administrators if they belong to any specified privilege groups.
  - Uses an async-safe lock and in-memory cache to deduplicate records (keyed by host and username).
  - Supports configuration for host list, SSH username, SSH private key (or its path), and privilege group names.
  - Does not embed passwords in the configuration, using key-based authentication instead.
  
Security:
  - Uses SSH key–based authentication for secure, encrypted connections.
  - Sensitive credentials (private key) should be stored securely (e.g., in environment variables or a secure secrets manager).

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import asyncssh
import logging
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger("LinuxUnixConnector")
logger.setLevel(logging.DEBUG)

class LinuxUnixConnector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the Linux/Unix Connector with the provided configuration.
        
        Expected configuration keys:
          - hosts: List of hostnames/IPs to scan.
          - ssh_username: SSH username for authentication.
          - ssh_private_key_path: Path to the SSH private key file for authentication.
              Alternatively, you can use "ssh_private_key" to supply the key contents directly.
          - privilege_groups: List of group names that indicate elevated privileges (default: ["sudo", "wheel"]).
          - max_workers: Maximum number of concurrent hosts to scan.
          - api_timeout: Timeout (in seconds) for SSH commands.
          - last_run: (Optional) ISO 8601 timestamp for incremental discovery (not implemented in this example).
          - kafka_topics: (Optional) Kafka topic settings for publishing discovered identities.
        """
        self.config = config
        self.hosts = config.get("hosts", [])
        self.ssh_username = config.get("ssh_username")
        # Retrieve the SSH private key either from a file path or directly from config.
        self.ssh_private_key = None
        if "ssh_private_key" in config:
            self.ssh_private_key = config["ssh_private_key"]
        elif "ssh_private_key_path" in config:
            key_path = config["ssh_private_key_path"]
            try:
                with open(key_path, "r") as key_file:
                    self.ssh_private_key = key_file.read()
            except Exception as e:
                logger.error("Failed to read SSH private key from %s: %s", key_path, e)
                raise
        else:
            raise ValueError("Configuration must include 'ssh_private_key_path' or 'ssh_private_key'.")
        
        self.privilege_groups = config.get("privilege_groups", ["sudo", "wheel"])
        self.max_workers = config.get("max_workers", 10)
        self.api_timeout = config.get("api_timeout", 10)
        self.last_run = config.get("last_run")  # Optional; not used in this basic example.
        self.kafka_topics = config.get("kafka_topics", {"identity": "linuxunix-identity"})
        
        # Deduplication: use a set keyed by "host:username"
        self.cache = set()
        self.lock = asyncio.Lock()
        
        logger.info("LinuxUnixConnector initialized for hosts: %s", self.hosts)
    
    async def fetch_host_data(self, host: str) -> List[Dict[str, Any]]:
        """
        Connects to a single host via SSH using key-based authentication,
        retrieves /etc/passwd and /etc/group, and returns identity records.
        
        :param host: Hostname or IP address of the Linux/Unix system.
        :return: A list of identity records for that host.
        """
        identities = []
        try:
            async with asyncssh.connect(
                host,
                username=self.ssh_username,
                client_keys=[self.ssh_private_key],
                known_hosts=None,
                timeout=self.api_timeout
            ) as conn:
                # Fetch /etc/passwd and /etc/group concurrently.
                passwd_task = conn.run("cat /etc/passwd", check=True)
                group_task = conn.run("cat /etc/group", check=True)
                passwd_result, group_result = await asyncio.gather(passwd_task, group_task)
                passwd_data = passwd_result.stdout
                group_data = group_result.stdout
                
                users = self.parse_passwd(passwd_data)
                groups = self.parse_group(group_data)
                
                # Merge group memberships into each user record.
                for user in users:
                    username = user.get("username")
                    user_groups = []
                    for grp in groups:
                        # Check if the username appears in the group's member list or if user's primary GID matches group's GID.
                        if username in grp.get("members", []):
                            user_groups.append(grp.get("group_name"))
                        elif user.get("gid") == grp.get("gid"):
                            user_groups.append(grp.get("group_name"))
                    user["groups"] = list(set(user_groups))
                    # Determine if the user has elevated privileges.
                    user["is_admin"] = any(g in self.privilege_groups for g in user["groups"])
                    # Tag the record with source and host.
                    user["objectType"] = "user"
                    user["source"] = "linuxunix"
                    user["host"] = host
                    # Deduplication: Unique key is host + username.
                    unique_key = f"{host}:{username}"
                    async with self.lock:
                        if unique_key in self.cache:
                            continue
                        self.cache.add(unique_key)
                    identities.append(user)
        except Exception as e:
            logger.error("Error fetching data from host %s: %s", host, e)
        return identities

    def parse_passwd(self, data: str) -> List[Dict[str, Any]]:
        """
        Parses /etc/passwd data into a list of user records.
        
        Each line in /etc/passwd has the format:
          username:password:UID:GID:gecos:home_directory:shell
        
        :param data: The content of /etc/passwd.
        :return: A list of dictionaries representing users.
        """
        users = []
        lines = data.strip().splitlines()
        for line in lines:
            parts = line.split(":")
            if len(parts) < 7:
                continue
            try:
                uid = int(parts[2])
                gid = int(parts[3])
            except ValueError:
                uid, gid = None, None
            user = {
                "username": parts[0],
                "uid": uid,
                "gid": gid,
                "comment": parts[4],
                "home": parts[5],
                "shell": parts[6]
            }
            users.append(user)
        return users

    def parse_group(self, data: str) -> List[Dict[str, Any]]:
        """
        Parses /etc/group data into a list of group records.
        
        Each line in /etc/group has the format:
          group_name:password:GID:member1,member2,...
        
        :param data: The content of /etc/group.
        :return: A list of dictionaries representing groups.
        """
        groups = []
        lines = data.strip().splitlines()
        for line in lines:
            parts = line.split(":")
            if len(parts) < 4:
                continue
            try:
                gid = int(parts[2])
            except ValueError:
                gid = None
            members = parts[3].split(",") if parts[3] else []
            group = {
                "group_name": parts[0],
                "gid": gid,
                "members": [m.strip() for m in members if m.strip()]
            }
            groups.append(group)
        return groups

    async def _async_fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches identity records from all configured Linux/Unix hosts.
        
        Returns:
            List[Dict[str, Any]]: Combined identity records from all hosts.
        """
        tasks = [self.fetch_host_data(host) for host in self.hosts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_identities = []
        for result in results:
            if isinstance(result, Exception):
                logger.error("Error in fetching host data: %s", result)
            else:
                all_identities.extend(result)
        logger.info("Fetched a total of %d identity records from Linux/Unix hosts.", len(all_identities))
        return all_identities

    def fetch_identities(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper to fetch identity records.
        
        Returns:
            List[Dict[str, Any]]: List of identity records.
        """
        return asyncio.run(self._async_fetch_identities())

if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Example configuration for Linux/Unix discovery using SSH key-based authentication.
    test_config = {
        "hosts": ["192.168.1.100", "192.168.1.101"],
        "ssh_username": "your_ssh_username",
        # Use either "ssh_private_key" (the key contents) or "ssh_private_key_path" (path to the key file)
        "ssh_private_key_path": "/path/to/your/private_key",  
        "privilege_groups": ["sudo", "wheel"],
        "max_workers": 10,
        "api_timeout": 10,
        "last_run": "2023-01-01T00:00:00Z",
        "kafka_topics": {
            "identity": "linuxunix-identity"
        }
    }
    
    connector = LinuxUnixConnector(test_config)
    identities = connector.fetch_identities()
    print("Linux/Unix Identities:")
    print(identities)
