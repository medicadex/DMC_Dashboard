
import sys
import os
print("Starting test")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("Trying to import db_utils")
try:
    from db_utils import get_db_engine
    print("db_utils imported")
    print("Creating engine")
    engine = get_db_engine()
    print("Engine created")
    print("Testing connection")
    with engine.connect() as conn:
        print("Connection successful")
        result = conn.execute("SELECT 1")
        print("Query result:", result.fetchone())
except Exception as e:
    print("Error:", e)
    import traceback
    traceback.print_exc()
