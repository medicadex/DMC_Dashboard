import os
import sys
from sqlalchemy import text
from db_utils import get_db_engine

def kill_locks():
    engine = get_db_engine()
    
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        print("Fetching processlist...")
        result = conn.execute(text("SHOW FULL PROCESSLIST")).fetchall()
        for r in result:
            pid = r[0]
            user = r[1]
            host = r[2]
            db = r[3]
            command = r[4]
            time = r[5]
            info = r[7]
            # Don't kill our own connection or system connections
            if user != 'rdsadmin' and time is not None and time > 10:
                print(f"Killing process {pid} (Command: {command}, Time: {time})...")
                try:
                    conn.execute(text(f"KILL {pid}"))
                except Exception as e:
                    print(f"Could not kill {pid}: {e}")

if __name__ == "__main__":
    kill_locks()
