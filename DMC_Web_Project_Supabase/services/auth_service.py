from utils.security import SecurityManager # type: ignore
from datetime import datetime, timedelta
import logging

class AuthService:
    def __init__(self, staff_repo, session_manager, supabase_client=None):
        self.repo = staff_repo
        self.session = session_manager
        self.supabase = supabase_client

    def log_attempt(self, username, action, details=None):
        """Encapsulated method for external UI components to log auth attempts."""
        self.repo.log_activity(username, action, details, event_type='MINOR')

    def login(self, username, password):
        # Requirements: Supabase Auth uses email. Map username -> email
        email = f"{username.lower()}@ikejaelectric.com"
        password_str = str(password).lower()

        # 1. Attempt Supabase Auth Login
        if self.supabase:
            try:
                res = self.supabase.auth.sign_in_with_password({"email": email, "password": password_str})
                if res.user:
                    # Success! Fetch extra details from staff table
                    user = self.repo.get_user_by_username(username)
                    if user:
                        self.repo.update_last_online_login(username)
                        self.session.reset_login_attempts(username)
                        self.session.update_activity()
                        self.repo.log_activity(username, "SUPABASE_LOGIN_SUCCESS", event_type='MAJOR')
                        
                        return self._build_user_dict(user)
            except Exception as e:
                # If user doesn't exist in Supabase yet, attempt "On-the-fly Migration"
                logging.info(f"Supabase login failed for {username}, attempting migration: {e}")

        # 2. On-the-fly Migration / Fallback Login
        user = self.repo.get_user_by_username(username)
        if not user:
            self.session.track_login_attempt(username)
            self.repo.log_activity(username, "LOGIN_FAILED", "User not found", event_type='MAJOR')
            return None

        # Check legacy password
        stored_pwd = user.get('password_hash') if isinstance(user, dict) else (user._mapping.get('password_hash') if hasattr(user, '_mapping') else user['password_hash'])
        is_valid = False
        
        try:
            if stored_pwd and stored_pwd.startswith("$2b$"): # Bcrypt signature
                is_valid = SecurityManager.verify_password(password_str, stored_pwd)
            else:
                is_valid = (password_str == stored_pwd)
        except Exception as e:
            logging.error(f"Legacy Hash Verification Error: {e}")
            is_valid = False

        if is_valid:
            # Legacy password matches! Migrate to Supabase Auth if client is available
            if self.supabase:
                try:
                    # Create user in Supabase Auth
                    # Note: Using sign_up. If they already exist, this might fail or send email depending on config.
                    # Better to use service role to create users without email confirmation if possible.
                    self.supabase.auth.sign_up({"email": email, "password": password_str})
                    logging.info(f"Successfully migrated {username} to Supabase Auth")
                except Exception as mig_err:
                    logging.error(f"Migration error for {username}: {mig_err}")

            self.repo.update_last_online_login(username)
            self.session.reset_login_attempts(username)
            self.session.update_activity()
            self.repo.log_activity(username, "LOGIN_SUCCESS_MIGRATED", event_type='MAJOR')
            
            return self._build_user_dict(user)
        else:
            self.session.track_login_attempt(username)
            self.repo.log_activity(username, "LOGIN_FAILED", "Invalid password", event_type='MAJOR')
            return None

    def _build_user_dict(self, user):
        u_mapping = user if isinstance(user, dict) else (user._mapping if hasattr(user, '_mapping') else user)
        fname = u_mapping.get('first_name', '') if hasattr(u_mapping, 'get') else u_mapping['first_name']
        sname = u_mapping.get('surname', '') if hasattr(u_mapping, 'get') else u_mapping['surname']
        synth_full_name = f"{fname or ''} {sname or ''}".strip()
        
        return {
            "id": u_mapping.get('id') if hasattr(u_mapping, 'get') else u_mapping['id'],
            "username": u_mapping.get('username') if hasattr(u_mapping, 'get') else u_mapping['username'],
            "staff_id": u_mapping.get('staff_id') if hasattr(u_mapping, 'get') else u_mapping['staff_id'],
            "full_name": synth_full_name,
            "role": u_mapping.get('role') if hasattr(u_mapping, 'get') else u_mapping['role']
        }

    def logout(self, username):
        if self.supabase:
            try:
                self.supabase.auth.sign_out()
            except:
                pass
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

    def reset_password_via_staff_id(self, username, staff_id, new_password):
        """Resets password after verifying staff_id."""
        user = self.repo.get_user_full_by_username(username)
        if not user:
            return False, "User not found."
            
        stored_staff_id = getattr(user, 'staff_id', None)
        if not stored_staff_id:
            try:
                stored_staff_id = user._mapping.get('staff_id') if hasattr(user, '_mapping') else user['staff_id']
            except:
                pass
                
        if not stored_staff_id or str(stored_staff_id).lower().replace(" ", "") != str(staff_id).lower().replace(" ", ""):
            return False, "Invalid Staff ID provided."
            
        # 2. Minimum Password Requirement
        if len(new_password) < 4:
            return False, "Password must be at least 4 characters long."

        # 3. Check Password History
        user_id = getattr(user, 'id', None)
        if not user_id:
            user_id = user._mapping.get('id') if hasattr(user, '_mapping') else user['id']

        history = self.repo.get_password_history(user_id)
        for old_hash in history:
            if SecurityManager.verify_password(new_password, old_hash):
                return False, "Cannot reuse one of the last 3 passwords."
            
        # 4. Update Password
        new_hash = SecurityManager.hash_password(new_password)
        try:
            self.repo.update_staff_password(username, new_hash)
            self.repo.add_password_to_history(user_id, new_hash)
            self.repo.log_activity(username, "PWD_RESET_SUCCESS", "Reset via Staff ID verification", event_type='MAJOR')
            self.session.reset_login_attempts(username)
            return True, "Password reset successfully."
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
            
        # 2. Minimum Password Requirement
        if len(new_password) < 4:
            return False, "Password must be at least 4 characters long."

        # 3. Check Password History
        user_id = getattr(user, 'id', None)
        if not user_id:
            user_id = user._mapping.get('id') if hasattr(user, '_mapping') else user['id']

        history = self.repo.get_password_history(user_id)
        for old_hash in history:
            if SecurityManager.verify_password(new_password, old_hash):
                return False, "Cannot reuse one of the last 3 passwords."
            
        # 4. Update Password
        new_hash = SecurityManager.hash_password(new_password)
        try:
            self.repo.update_staff_password(username, new_hash)
            self.repo.add_password_to_history(user_id, new_hash)
            self.repo.log_activity(username, "PWD_CHANGE_SUCCESS", event_type='MAJOR')
            return True, "Password changed successfully."
        except Exception as e:
            return False, f"Failed to update password: {str(e)}"
