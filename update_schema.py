import os
from sqlalchemy import text
from db_utils import get_db_engine

def update_schema():
    engine = get_db_engine()
    with engine.begin() as conn:
        try:
            print("Dropping unique index on username...")
            conn.execute(text("ALTER TABLE staff DROP INDEX username"))
        except Exception as e:
            print("Warning (username index might not exist):", e)
            
        try:
            print("Adding unique index on staff_id...")
            conn.execute(text("ALTER TABLE staff ADD UNIQUE INDEX unique_staff_id (staff_id)"))
        except Exception as e:
            print("Warning (staff_id index might already exist):", e)
            
        try:
            print("Updating usernames based on email...")
            conn.execute(text("UPDATE staff SET username = SUBSTRING_INDEX(email, '@', 1) WHERE email IS NOT NULL AND email != ''"))
            conn.execute(text("UPDATE staff SET username = NULL WHERE email IS NULL OR email = ''"))
            print("Schema and data successfully updated.")
        except Exception as e:
            print("Error updating data:", e)

if __name__ == "__main__":
    update_schema()
