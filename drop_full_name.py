import os
from sqlalchemy import text
from db_utils import get_db_engine

def drop_full_name():
    engine = get_db_engine()
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE staff DROP COLUMN full_name"))
            print("Successfully dropped 'full_name' column from 'staff' table.")
        except Exception as e:
            print("Error:", e)

if __name__ == "__main__":
    drop_full_name()
