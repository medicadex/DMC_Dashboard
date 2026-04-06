from utils.security import SecurityManager # type: ignore
from datetime import datetime, timedelta
from db_utils import is_online # type: ignore

class AuthService:
    def __init__(self, staff_repo, session_manager):
        self.repo = staff_repo
        self.session = session_manager
        self.OFFLINE_GRACE_DAYS = 7 # Requirement: 3-7 days

    def log_attempt(self, username, action, details=None):
        """Encapsulated method for external UI components to log auth attempts."""
        self.repo.log_activity(username, action, details, event_type='MINOR')

    def login(self, username, password):
        if self.session.is_account_locked(username):
            raise Exception("Account locked due to too many failed attempts.")

        user = self.repo.get_user_by_username(username)
        if not user:
            self.session.track_login_attempt(username)
            self.repo.log_activity(username, "LOGIN_FAILED", "User not found", event_type='MAJOR')
            return None

        # Offline Grace Period Enforcement
        if not is_online():
            # Check if user has last_online_login
            last_login = user.get('last_online_login')
            if not last_login:
                raise Exception("Offline login not permitted. Please log in online first to authorize this device.")
            
            # Ensure last_login is a datetime object (SQLite might return string)
            if isinstance(last_login, str):
                try: last_login = datetime.fromisoformat(last_login)
                except: last_login = None
            
            if not last_login or (datetime.now() - last_login) > timedelta(days=self.OFFLINE_GRACE_DAYS):
                raise Exception(f"Offline access expired (Limit: {self.OFFLINE_GRACE_DAYS} days). Please connect to the internet to re-authorize.")

        # Check if password is bcrypt hashed or plain (for migration)
        stored_pwd = user.get('password_hash')
        is_valid = False
        
        try:
            if stored_pwd and stored_pwd.startswith("$2b$"): # Bcrypt signature
                is_valid = SecurityManager.verify_password(password, stored_pwd)
            else:
                # Temporary plain text check for migration
                is_valid = (password == stored_pwd)
                if is_valid:
                    # Auto-upgrade to hashed password
                    new_hash = SecurityManager.hash_password(password)
                    self.repo.update_staff_password(username, new_hash) # Use specific update method
        except Exception as e:
            print(f"Login Hash Verification Error: {e}")
            is_valid = False

        if is_valid:
            # Update last_online_login if currently online
            if is_online():
                self.repo.update_last_online_login(username)

            self.session.reset_login_attempts(username)
            self.session.update_activity()
            self.repo.log_activity(username, "LOGIN_SUCCESS", event_type='MAJOR')
            return {
                "id": user.get('id'),
                "username": user.get('username'),
                "full_name": user.get('full_name'),
                "role": user.get('role')
            }
        else:
            self.session.track_login_attempt(username)
            self.repo.log_activity(username, "LOGIN_FAILED", "Invalid password", event_type='MAJOR')
            return None

    def logout(self, username):
        self.repo.log_activity(username, "LOGOUT", event_type='MAJOR')

    def reset_password_to_default(self, email):
        """Resets password to staff_id if email exists."""
        user = self.repo.get_user_by_email(email)
        if not user:
            return False, "Email address not found in our records."
        
        # Reset to staff_id (default if staff_id exists, fallback to username)
        # Use lower() for consistency
        try:
            # Check for attribute access first
            default_pwd = getattr(user, 'staff_id', None)
            if not default_pwd:
                default_pwd = getattr(user, 'username', None)
        except:
            # Fallback to index-based access
            # SELECT id, username, staff_id ...
            try:
                default_pwd = user[2] if user[2] else user[1]
            except:
                default_pwd = user[1]

        default_pwd = str(default_pwd).lower()
        new_hash = SecurityManager.hash_password(default_pwd)
        
        try:
            self.repo.update_staff_password(user.username, new_hash)
            self.repo.log_activity(user.username, "PASSWORD_RESET", f"Reset to default via email: {email}", event_type='MAJOR')
            return True, "Password reset successful! Your new password is your Staff ID."
        except Exception as e:
            return False, f"Failed to reset password: {str(e)}"

    def change_password(self, username, current_password, new_password):
        """Securely changes a user's password with validation."""
        user = self.repo.get_user_full_by_username(username)
        if not user:
            return False, "User not found."
            
        # 1. Validate Current Password
        stored_pwd = user.password_hash
        if stored_pwd.startswith("$2b$"):
            is_valid = SecurityManager.verify_password(current_password, stored_pwd)
        else:
            is_valid = (current_password == stored_pwd)
            
        if not is_valid:
            self.repo.log_activity(username, "PWD_CHANGE_FAIL", "Invalid current password", event_type='MAJOR')
            return False, "Incorrect current password."
            
        # 2. Enforce Strong Password Requirements
        if len(new_password) < 8:
            return False, "Password must be at least 8 characters long."
        
        import re
        if not re.search(r"[A-Z]", new_password):
            return False, "Password must contain at least one uppercase letter."
        if not re.search(r"[a-z]", new_password):
            return False, "Password must contain at least one lowercase letter."
        if not re.search(r"\d", new_password):
            return False, "Password must contain at least one number."
        if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", new_password):
            return False, "Password must contain at least one special character."

        # 3. Check Password History
        history = self.repo.get_password_history(user.id)
        for old_hash in history:
            if SecurityManager.verify_password(new_password, old_hash):
                return False, "Cannot reuse one of the last 3 passwords."
            
        # 4. Update Password
        new_hash = SecurityManager.hash_password(new_password)
        try:
            self.repo.update_staff_password(username, new_hash)
            self.repo.add_password_to_history(user.id, new_hash)
            self.repo.log_activity(username, "PWD_CHANGE_SUCCESS", event_type='MAJOR')
            return True, "Password changed successfully."
        except Exception as e:
            return False, f"Failed to update password: {str(e)}"
