#!/usr/bin/env python3
"""
rbac.py

This module implements Role-Based Access Control (RBAC) for the AI-Powered Identity Risk Analytics Platform.
It loads role-to-permission mappings from a YAML configuration file and provides functions to assign roles,
retrieve user roles, and check whether a user has specific permissions.

Author: [Your Name]
Date: [Current Date]
"""

import os
import yaml

# Global dictionary to store user role assignments.
# In a production system, this might be replaced with a database.
USER_ROLES = {}

def load_rbac_config(config_path: str) -> dict:
    """
    Loads the RBAC configuration from a YAML file.

    Parameters:
        config_path (str): The path to the RBAC configuration YAML file.

    Returns:
        dict: A dictionary representing the role-permission mappings.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"RBAC configuration file not found at {config_path}")
    
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

# Load RBAC configuration from the expected location.
# This path assumes that the auth-service is being run from its own directory.
RBAC_CONFIG_PATH = os.getenv("RBAC_CONFIG_PATH", "services/common-services/auth-service/config/rbac_config.yml")
RBAC_CONFIG = load_rbac_config(RBAC_CONFIG_PATH)

def assign_role(user_id: int, role: str) -> None:
    """
    Assigns a role to a user. If the user already has roles, the new role is added.

    Parameters:
        user_id (int): The unique identifier of the user.
        role (str): The role to assign.
    """
    if role not in RBAC_CONFIG.get("roles", {}):
        raise ValueError(f"Role '{role}' is not defined in the RBAC configuration.")
    
    if user_id in USER_ROLES:
        if role not in USER_ROLES[user_id]:
            USER_ROLES[user_id].append(role)
    else:
        USER_ROLES[user_id] = [role]

def remove_role(user_id: int, role: str) -> None:
    """
    Removes a role from a user.

    Parameters:
        user_id (int): The unique identifier of the user.
        role (str): The role to remove.
    """
    if user_id in USER_ROLES:
        if role in USER_ROLES[user_id]:
            USER_ROLES[user_id].remove(role)

def get_user_roles(user_id: int) -> list:
    """
    Retrieves the list of roles assigned to a user.

    Parameters:
        user_id (int): The unique identifier of the user.

    Returns:
        list: A list of roles assigned to the user.
    """
    return USER_ROLES.get(user_id, [])

def get_effective_permissions(user_id: int) -> set:
    """
    Computes the effective permissions for a user based on their assigned roles.

    Parameters:
        user_id (int): The unique identifier of the user.

    Returns:
        set: A set of effective permissions granted to the user.
    """
    roles = get_user_roles(user_id)
    effective_permissions = set()
    
    for role in roles:
        role_data = RBAC_CONFIG.get("roles", {}).get(role, {})
        permissions = role_data.get("permissions", [])
        for perm in permissions:
            effective_permissions.add(perm)
    
    return effective_permissions

def has_permission(user_id: int, permission: str) -> bool:
    """
    Checks if a user has a specific permission.

    Parameters:
        user_id (int): The unique identifier of the user.
        permission (str): The permission to check (e.g., 'read:report').

    Returns:
        bool: True if the user has the permission, False otherwise.
    """
    effective_permissions = get_effective_permissions(user_id)
    # Simple check: exact match or wildcard match
    if permission in effective_permissions:
        return True
    # Handle wildcard permissions (e.g., 'read:*')
    for perm in effective_permissions:
        if perm.endswith(":*"):
            base_perm = perm.split(":")[0]
            if permission.startswith(base_perm + ":"):
                return True
    return False

if __name__ == "__main__":
    # Basic test of RBAC functionality.
    # This block simulates role assignment, retrieval, and permission checking.
    user_id_example = 1

    print("Loading RBAC configuration from:", RBAC_CONFIG_PATH)
    print("RBAC Configuration:", RBAC_CONFIG)

    # Assign roles to the user
    assign_role(user_id_example, "admin")
    assign_role(user_id_example, "editor")
    
    # Display assigned roles
    roles = get_user_roles(user_id_example)
    print(f"User {user_id_example} roles:", roles)
    
    # Retrieve effective permissions for the user
    permissions = get_effective_permissions(user_id_example)
    print(f"User {user_id_example} effective permissions:", permissions)
    
    # Check if the user has a specific permission
    test_permission = "write:content"
    if has_permission(user_id_example, test_permission):
        print(f"User {user_id_example} has permission: {test_permission}")
    else:
        print(f"User {user_id_example} does NOT have permission: {test_permission}")
