#!/usr/bin/env python3
"""
test_auth.py

Unit tests for the authentication module (auth.py) of the AI-Powered Identity Risk Analytics Platform.
Tests cover token generation, token verification, and the user login process.

Author: [Your Name]
Date: [Current Date]
"""

import os
import sys
import time
import unittest
import jwt
from jwt import ExpiredSignatureError

# Adjust the system path to ensure the auth module can be imported.
# This assumes the test file is located in services/common-services/auth-service/tests/
# and the auth.py file is located in services/common-services/auth-service/src/
import pathlib
current_dir = pathlib.Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(current_dir))

import auth  # Now we can import our auth module

class TestAuthService(unittest.TestCase):
    def setUp(self):
        """
        Set up test environment for auth service.
        Ensures environment variables for authentication are set.
        """
        # Set environment variables needed by auth.py
        os.environ["AUTH_SECRET_KEY"] = "test_secret_key"
        os.environ["AUTH_ALGORITHM"] = "HS256"
        os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = "1"   # Short expiry for testing purposes
        os.environ["REFRESH_TOKEN_EXPIRE_DAYS"] = "1"       # Short expiry for testing purposes

        self.test_user_data = {"user_id": 1, "username": "admin"}

    def tearDown(self):
        """
        Clean up any modifications to the environment.
        """
        # Optionally, remove environment variables set during tests.
        os.environ.pop("AUTH_SECRET_KEY", None)
        os.environ.pop("AUTH_ALGORITHM", None)
        os.environ.pop("ACCESS_TOKEN_EXPIRE_MINUTES", None)
        os.environ.pop("REFRESH_TOKEN_EXPIRE_DAYS", None)

    def test_generate_access_token(self):
        """
        Test that an access token is generated and contains the correct payload.
        """
        token = auth.generate_access_token(self.test_user_data)
        self.assertIsInstance(token, str)
        
        # Decode the token to verify its contents.
        payload = jwt.decode(token, os.environ["AUTH_SECRET_KEY"], algorithms=[os.environ["AUTH_ALGORITHM"]])
        self.assertEqual(payload["user_id"], self.test_user_data["user_id"])
        self.assertEqual(payload["username"], self.test_user_data["username"])

    def test_generate_refresh_token(self):
        """
        Test that a refresh token is generated and contains the correct payload.
        """
        token = auth.generate_refresh_token({"user_id": self.test_user_data["user_id"]})
        self.assertIsInstance(token, str)
        payload = jwt.decode(token, os.environ["AUTH_SECRET_KEY"], algorithms=[os.environ["AUTH_ALGORITHM"]])
        self.assertEqual(payload["user_id"], self.test_user_data["user_id"])

    def test_verify_token_valid(self):
        """
        Test that a valid access token is correctly verified.
        """
        token = auth.generate_access_token(self.test_user_data)
        payload = auth.verify_token(token)
        self.assertEqual(payload["user_id"], self.test_user_data["user_id"])
        self.assertEqual(payload["username"], self.test_user_data["username"])

    def test_verify_token_expired(self):
        """
        Test that an expired token raises an ExpiredSignatureError.
        """
        # Temporarily set the access token expiry to 0 minutes (immediate expiry)
        original_expiry = os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES")
        os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = "0"
        
        token = auth.generate_access_token(self.test_user_data)
        # Wait a short moment to ensure the token has expired
        time.sleep(1)
        with self.assertRaises(ExpiredSignatureError):
            auth.verify_token(token)
        
        # Restore the original expiry value
        os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = original_expiry

    def test_login_user_success(self):
        """
        Test that login_user returns valid tokens for correct credentials.
        """
        # According to auth.py, correct credentials are "admin" and "password123"
        result = auth.login_user("admin", "password123")
        self.assertIn("access_token", result)
        self.assertIn("refresh_token", result)
        self.assertEqual(result.get("token_type"), "bearer")

    def test_login_user_failure(self):
        """
        Test that login_user raises an exception for incorrect credentials.
        """
        with self.assertRaises(Exception) as context:
            auth.login_user("admin", "incorrect_password")
        self.assertIn("Invalid", str(context.exception))

if __name__ == "__main__":
    unittest.main()
