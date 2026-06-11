import os
import sys
import requests
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_utils import get_db_engine

load_dotenv()

def migrate_users():
    # 1. Init variables
    url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not service_key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
        return

    engine = get_db_engine()

    print("Fetching users from 'staff' table...")
    with engine.connect() as conn:
        users = conn.execute(text("SELECT username, staff_id, role FROM staff")).fetchall()

    print(f"Found {len(users)} users to migrate.")

    success_count = 0
    fail_count = 0

    # HTTP API endpoint for creating users (more reliable than SDK admin methods sometimes)
    api_url = f"{url}/auth/v1/admin/users"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json"
    }

    for user in users:
        username = user[0]
        staff_id = user[1]
        role = user[2]

        if not username:
            print(f"  [SKIP] User has NULL username, skipping.")
            continue

        email = f"{username.lower()}@ikejaelectric.com"
        initial_password = str(staff_id).lower()

        print(f"Migrating {username} ({email})...")
        try:
            # Direct HTTP request to Supabase Auth Admin API
            payload = {
                "email": email,
                "password": initial_password,
                "email_confirm": True,
                "user_metadata": {
                    "username": username,
                    "role": role,
                    "staff_id": staff_id
                }
            }
            
            response = requests.post(api_url, json=payload, headers=headers)
            
            if response.status_code == 201:
                print(f"  [SUCCESS] Created {username}")
                success_count += 1
            elif response.status_code == 422 or "already exists" in response.text:
                print(f"  [INFO] {username} already exists in Supabase Auth.")
                success_count += 1
            else:
                print(f"  [ERROR] Failed to migrate {username}: {response.status_code} - {response.text}")
                fail_count += 1
                
        except Exception as e:
            print(f"  [ERROR] Failed to migrate {username}: {str(e)}")
            fail_count += 1

    print("\nMigration Complete!")
    print(f"Total Success: {success_count}")
    print(f"Total Failed: {fail_count}")
    print("\nUsers can now log in with their IE username and Staff ID as password.")
    print("Their sessions will be managed by Supabase Auth.")
    print("\nRefresh the Supabase Dashboard to see your users!")

if __name__ == "__main__":
    migrate_users()
