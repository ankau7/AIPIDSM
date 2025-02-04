#!/usr/bin/env python3
"""
auth.py

This module provides full authentication functionality for the AI-Powered Identity Risk Analytics Platform.
It implements user login, JWT access token generation, refresh token generation, and token verification.
Authentication is based on JSON Web Tokens (JWT) using the PyJWT library.
All configuration values (e.g., SECRET_KEY, token expiration times) are read from environment variables,
allowing flexible deployment across different environments.

Author: [Your Name]
Date: [Current Date]
"""

import os
import jwt
import datetime
from jwt import ExpiredSignatureError, InvalidTokenError

# Configuration: These constants can be overridden by environment variables.
SECRET_KEY = os.getenv("AUTH_SECRET_KEY", "your_default_secret_key")
ALGORITHM = os.getenv("AUTH_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

def generate_access_token(data: dict) -> str:
    """
    Generates a JWT access token with an expiration time.
    
    Parameters:
        data (dict): A dictionary containing the data to encode in the token (e.g., user_id, username).
    
    Returns:
        str: A JWT access token.
    """
    # Copy the input data to avoid modifying the original dictionary.
    to_encode = data.copy()
    # Calculate the token expiration time.
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    # Encode the JWT token with the specified algorithm.
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return token

def generate_refresh_token(data: dict) -> str:
    """
    Generates a JWT refresh token with an expiration time.
    
    Parameters:
        data (dict): A dictionary containing the data to encode in the token (e.g., user_id).
    
    Returns:
        str: A JWT refresh token.
    """
    to_encode = data.copy()
    expire = datetime.datetime.utcnow() + datetime.timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return token

def verify_token(token: str) -> dict:
    """
    Verifies a JWT token and returns its payload if valid.
    
    Parameters:
        token (str): The JWT token to verify.
    
    Returns:
        dict: The decoded token payload.
    
    Raises:
        ExpiredSignatureError: If the token has expired.
        InvalidTokenError: If the token is invalid.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except ExpiredSignatureError as e:
        raise ExpiredSignatureError("Token has expired.") from e
    except InvalidTokenError as e:
        raise InvalidTokenError("Invalid token.") from e

def login_user(username: str, password: str) -> dict:
    """
    Authenticates a user based on provided credentials.
    
    This function simulates authentication by checking against hardcoded credentials.
    In a production system, this function should verify the username and password
    against a user database or identity provider.
    
    Parameters:
        username (str): The username provided by the user.
        password (str): The password provided by the user.
    
    Returns:
        dict: A dictionary containing an access token, a refresh token, and token type information.
    
    Raises:
        Exception: If the credentials are invalid.
    """
    # Dummy authentication logic: replace with database verification in production.
    if username == "admin" and password == "password123":
        user_data = {"user_id": 1, "username": username}
        access_token = generate_access_token(user_data)
        refresh_token = generate_refresh_token({"user_id": 1})
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }
    else:
        raise Exception("Invalid username or password.")

if __name__ == "__main__":
    # If this module is run as a script, simulate a login and token verification.
    try:
        # Simulate a login attempt with hardcoded credentials.
        credentials = {"username": "admin", "password": "password123"}
        tokens = login_user(credentials["username"], credentials["password"])
        print("Login successful!")
        print("Access Token:", tokens["access_token"])
        print("Refresh Token:", tokens["refresh_token"])

        # Verify the access token.
        payload = verify_token(tokens["access_token"])
        print("Verified Token Payload:", payload)
    except Exception as e:
        print("Authentication error:", e)
