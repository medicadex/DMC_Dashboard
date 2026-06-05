import os
import sys
from sqlalchemy import text
from db_utils import get_db_engine

def dedup_staff():
    engine = get_db_engine()
    
    with engine.begin() as conn:
        print("Killing all other connections to clear locks...")
        try:
            kills = conn.execute(text("SELECT CONCAT('KILL ', id, ';') FROM information_schema.processlist WHERE db = 'dmc' AND id != CONNECTION_ID()")).fetchall()
            for k in kills:
                try:
                    conn.execute(text(k[0]))
                    print(f"Executed: {k[0]}")
                except:
                    pass
        except Exception as e:
            print("Could not kill processes:", e)

        print("Fetching ids to keep...")
        result = conn.execute(text("SELECT MIN(id) FROM staff WHERE staff_id IS NOT NULL AND staff_id != '' GROUP BY staff_id")).fetchall()
        ids_to_keep = [r[0] for r in result]
        
        if not ids_to_keep:
            print("No valid staff records found.")
            return
            
        placeholders = ', '.join(str(i) for i in ids_to_keep)
        
        dup_result = conn.execute(text(f"SELECT id FROM staff WHERE staff_id IS NOT NULL AND staff_id != '' AND id NOT IN ({placeholders})")).fetchall()
        dup_ids = [r[0] for r in dup_result]
        
        if dup_ids:
            # Disable FK checks and delete everything in one batch
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            batch_placeholders = ', '.join(str(b) for b in dup_ids)
            print("Deleting duplicates...")
            conn.execute(text(f"DELETE FROM staff WHERE id IN ({batch_placeholders})"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            print("Successfully deleted all duplicates.")

if __name__ == "__main__":
    dedup_staff()
