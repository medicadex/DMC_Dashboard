from sqlalchemy import text
from datetime import datetime
import logging
import uuid
from utils.security import SecurityManager # type: ignore

class StaffRepository:
    def __init__(self, engine):
        self.engine = engine

    def _process_user_row(self, row):
        """Decrypts sensitive fields if reading from database."""
        if not row: return None
        
        # SQLAlchemy 2.0 Row objects have a _mapping property, while 1.4/LegacyRow also support it.
        # Dictionaries do not. We need a robust way to convert to dict.
        if isinstance(row, dict):
            data = row.copy()
        elif hasattr(row, '_mapping'):
            data = dict(row._mapping)
        else:
            # Fallback for older SQLAlchemy or cases where _mapping is missing
            try:
                data = dict(row)
            except (TypeError, ValueError):
                # Final fallback for custom row objects or tuple-like objects
                try:
                    # In some SQLAlchemy versions, row.keys() returns column names
                    keys = row.keys()
                    data = {k: row[i] for i, k in enumerate(keys)}
                except:
                    # If all else fails, return as is (but this might cause issues downstream)
                    return row
        
        if 'email' in data and data['email']:
            data['email'] = SecurityManager.decrypt_data(data['email'])
        if 'phone_number' in data and data['phone_number']:
            data['phone_number'] = SecurityManager.decrypt_data(data['phone_number'])
            
        return data

    def get_user_by_username(self, username: str):
        with self.engine.connect() as conn:
            # Case-insensitive username lookup using LOWER()
            query = text("SELECT id, username, staff_id, password_hash, full_name, role, email, last_online_login FROM staff WHERE LOWER(username) = LOWER(:u)")
            row = conn.execute(query, {"u": username}).fetchone()
            return self._process_user_row(row)

    def get_user_by_email(self, email: str):
        with self.engine.connect() as conn:
            query = text("SELECT id, username, staff_id, password_hash, full_name, role, email, last_online_login FROM staff WHERE email = :e")
            row = conn.execute(query, {"e": email}).fetchone()
            return self._process_user_row(row)

    def log_activity(self, username: str, action: str, details: str = None, session_id: str = None, tab_id: str = None, event_type: str = 'MINOR'):
        trans_id = str(uuid.uuid4())
        
        with self.engine.begin() as conn:
            try:
                query = text("""
                    INSERT INTO user_activity_log (username, action, details, timestamp, session_id, tab_id, event_type, transaction_id, sync_status)
                    VALUES (:u, :a, :d, :t, :s, :tab, :et, :tid, 'SYNCED')
                """)
                conn.execute(query, {
                    "u": username, "a": action, "d": details, "t": datetime.now(), 
                    "s": session_id, "tab": tab_id, "et": event_type, "tid": trans_id
                })
            except Exception as e:
                logging.error(f"Failed to log activity: {e}")

    def get_all_staff(self):
        with self.engine.connect() as conn:
            return conn.execute(text("SELECT id, username, full_name, role FROM staff")).fetchall()

    def add_staff(self, username, hashed_pwd, full_name, role, email=None, phone_number=None):
        # Generate transaction ID
        trans_id = str(uuid.uuid4())
        
        try:
            with self.engine.begin() as conn:
                query = text("""
                    INSERT INTO staff (username, password_hash, full_name, role, email, phone_number, transaction_id, sync_status)
                    VALUES (:u, :p, :f, :r, :e, :ph, :tid, 'SYNCED')
                """)
                conn.execute(query, {
                    "u": username, "p": hashed_pwd, "f": full_name, "r": role,
                    "e": email, "ph": phone_number, "tid": trans_id
                })
        except Exception as e:
            logging.error(f"Failed to push new staff to RDS: {e}")

    def update_staff_password(self, username, hashed_pwd):
        try:
            with self.engine.begin() as conn:
                query = text("UPDATE staff SET password_hash = :p, sync_status = 'SYNCED' WHERE username = :u")
                conn.execute(query, {"p": hashed_pwd, "u": username})
        except Exception as e:
            logging.error(f"Failed to update staff password in RDS: {e}")

    def update_last_online_login(self, username):
        """Updates the last_online_login timestamp in cloud database."""
        now = datetime.now()
        try:
            with self.engine.begin() as conn:
                conn.execute(text("UPDATE staff SET last_online_login = :t WHERE username = :u"), {"t": now, "u": username})
        except Exception as e:
            logging.error(f"Failed to update last_online_login in RDS: {e}")

    def get_user_full_by_username(self, username: str):
        with self.engine.connect() as conn:
            # Case-insensitive lookup
            query = text("SELECT id, username, staff_id, password_hash, full_name, role, email, last_online_login FROM staff WHERE LOWER(username) = LOWER(:u)")
            row = conn.execute(query, {"u": username}).fetchone()
            return self._process_user_row(row)

    def delete_staff(self, staff_id):
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM staff WHERE id = :id"), {"id": staff_id})

    def get_activity_log(self, **filters):
        with self.engine.connect() as conn:
            sql = "SELECT timestamp, username, action, tab_id, details, event_type, session_id FROM user_activity_log WHERE 1=1"
            params = {}

            if filters.get("start") and filters.get("end"):
                sql += " AND timestamp BETWEEN :start AND :end"
                params["start"] = filters["start"]
                params["end"] = filters["end"]

            if filters.get("event_type") != "ALL":
                sql += " AND event_type = :et"
                params["et"] = filters["event_type"]

            if filters.get("username"):
                sql += " AND username LIKE :u"
                params["u"] = f"%{filters['username']}%"

            if filters.get("tab_id"):
                sql += " AND tab_id LIKE :t"
                params["t"] = f"%{filters['tab_id']}%"

            if filters.get("search"):
                sql += " AND (action LIKE :s OR details LIKE :s)"
                params["s"] = f"%{filters['search']}%"

            sql += " ORDER BY timestamp DESC LIMIT 1000"
            return conn.execute(text(sql), params).fetchall()

    def add_password_to_history(self, user_id, password_hash):
        with self.engine.begin() as conn:
            query = text("INSERT INTO password_history (user_id, password_hash) VALUES (:user_id, :password_hash)")
            conn.execute(query, {"user_id": user_id, "password_hash": password_hash})

    def get_password_history(self, user_id, limit=3):
        with self.engine.connect() as conn:
            query = text("SELECT password_hash FROM password_history WHERE user_id = :user_id ORDER BY created_at DESC LIMIT :limit")
            return [row[0] for row in conn.execute(query, {"user_id": user_id, "limit": limit})]
