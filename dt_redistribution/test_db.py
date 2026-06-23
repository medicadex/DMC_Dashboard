import sys
import os
import traceback

print("Starting test_db.py...")
print(f"Python version: {sys.version}")
print(f"Current directory: {os.getcwd()}")

try:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from db_utils import get_db_engine
    from sqlalchemy import text
    print("Imports successful!")

    print("Creating engine...")
    engine = get_db_engine()
    print("Engine created successfully!")

    print("Connecting to database...")
    with engine.connect() as conn:
        print("Connection established!")
        
        print("Executing query to get tables...")
        result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
        tables = [row[0] for row in result]
        print(f"Found {len(tables)} tables!")
        print("Tables in database:")
        for t in tables:
            print(f"- {t}")
            
        # Check customers table columns
        if 'customers' in tables:
            print("\nChecking customers table columns...")
            cols = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'customers'"))
            col_list = [c[0] for c in cols]
            print("Customers table columns:")
            for c in col_list:
                print(f"- {c}")

except Exception as e:
    print(f"Error occurred: {e}")
    print("Stack trace:")
    traceback.print_exc()

print("\nDone!")

