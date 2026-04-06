import hashlib
import bcrypt
import time
import os
import base64
from cryptography.fernet import Fernet

class SecurityManager:
    # A fixed, internal key for basic local data obfuscation
    # In a real-world scenario, this should be unique per installation
    _LOCAL_KEY = base64.urlsafe_b64encode(hashlib.sha256(b"DMC_OFFLINE_SECRET").digest())
    _FERNET = Fernet(_LOCAL_KEY)

    @staticmethod
    def hash_password(password: str) -> str:
        """Hashes a plain password using bcrypt."""
        # bcrypt expects bytes, and returns bytes. We store as string.
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')

    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """Verifies a plain password against a bcrypt hash."""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
        except Exception:
            return False

    @staticmethod
    def encrypt_data(data: str) -> str:
        """Encrypts sensitive local data for storage."""
        if not data: return ""
        return SecurityManager._FERNET.encrypt(data.encode()).decode()

    @staticmethod
    def decrypt_data(encrypted_data: str) -> str:
        """Decrypts sensitive local data."""
        if not encrypted_data: return ""
        try:
            return SecurityManager._FERNET.decrypt(encrypted_data.encode()).decode()
        except Exception:
            return encrypted_data # Return as-is if decryption fails

    @staticmethod
    def generate_dedup_hash(row_data: dict) -> str:
        """Generates a SHA-256 hash for deduplication of 'Other Payments'."""
        key = f"{row_data['account_number']}_{row_data['amount_paid']}_{row_data['date_of_payment']}"
        return hashlib.sha256(key.encode()).hexdigest()

class SessionManager:
    def __init__(self, timeout_minutes=15):
        self.timeout_seconds = timeout_minutes * 60
        self.last_activity = time.time()
        self.login_attempts = {}

    def update_activity(self):
        self.last_activity = time.time()

    def is_session_valid(self) -> bool:
        return (time.time() - self.last_activity) < self.timeout_seconds

    def track_login_attempt(self, username: str):
        attempts = self.login_attempts.get(username, 0) + 1
        self.login_attempts[username] = attempts
        return attempts

    def reset_login_attempts(self, username: str):
        if username in self.login_attempts:
            del self.login_attempts[username]

    def is_account_locked(self, username: str) -> bool:
        return self.login_attempts.get(username, 0) >= 5
