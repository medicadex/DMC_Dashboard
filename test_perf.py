import time
from app import app
with app.app_context():
    from flask import session
    from app import engine
    from sqlalchemy import text
    
    start_time = time.time()
    count_sql = text("""
        SELECT 
            COUNT(*) as total_rows,
            SUM(p.amount_paid) as total_amount
        FROM all_payments p
        JOIN customers c ON p.account_number = c.account_number
    """)
    with engine.connect() as conn:
        res = conn.execute(count_sql).fetchone()
    print('Time taken:', time.time() - start_time)
    print('Result:', res)
