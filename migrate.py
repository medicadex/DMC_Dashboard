import os
from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote_plus
import warnings
warnings.filterwarnings('ignore')

print("Starting Advanced Database Migration...")

# MySQL Connection (Source)
mysql_user = "admin"
mysql_pass = "Loveth123."
mysql_host = "100.24.75.156"
mysql_port = "3306"
mysql_db = "dmc"
mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{quote_plus(mysql_pass)}@{mysql_host}:{mysql_port}/{mysql_db}?charset=utf8mb4")

# PostgreSQL Connection (Destination)
pg_user = "postgres.ehfsoblniohalywhbbht"
pg_pass = "Adavize1104"
pg_host = "aws-0-eu-west-1.pooler.supabase.com"
pg_port = "5432"
pg_db = "postgres"
pg_engine = create_engine(f"postgresql+psycopg2://{pg_user}:{quote_plus(pg_pass)}@{pg_host}:{pg_port}/{pg_db}", connect_args={'sslmode': 'require'})

PRIMARY_KEYS = {
    'accounts': 'account_number',
    'adjustments': 'id',
    'app_users': 'id',
    'collections': 'id',
    'customer_officer_history': 'id',
    'customers': 'account_number',
    'disconnections': 'id',
    'discounts': 'id',
    'migration_meta': 'migration_name',
    'other_payments': 'id',
    'password_history': 'id',
    'performance_config': 'id',
    'resolutions': 'id',
    'staff': 'id',
    'staff_pending_updates': 'id',
    'user_activity_log': 'id',
    'validation': 'id'
}

VIEWS_TO_SKIP = ['account_financial_summary', 'all_payments']

with pg_engine.begin() as pg_conn:
    print("\n--- Cleaning up falsely migrated views ---")
    for v in VIEWS_TO_SKIP:
        pg_conn.execute(text(f"DROP TABLE IF EXISTS {v} CASCADE"))
        print(f"Dropped table {v} (if existed) so it can be created as a view later.")

with mysql_engine.connect() as mysql_conn:
    tables = [row[0] for row in mysql_conn.execute(text("SHOW TABLES"))]
    print(f"\nFound {len(tables)} tables/views in MySQL.")

    for table in tables:
        if table in VIEWS_TO_SKIP:
            print(f"Skipping {table} (Will be recreated as a view later).")
            continue
            
        print(f"\nMigrating table: {table} ...")
        
        # Check if table exists in PostgreSQL and has data
        skip_streaming = False
        with pg_engine.connect() as pg_conn:
            res = pg_conn.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table}')")).scalar()
            if res:
                count = pg_conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
                if count > 0:
                    print(f"  -> Table '{table}' already exists and contains {count} rows. Skipping data insertion.")
                    skip_streaming = True

        if not skip_streaming:
            try:
                df = pd.read_sql_table(table, mysql_conn)
                if df.empty:
                    print(f"  -> Table '{table}' is empty in MySQL.")
                    df.to_sql(table, pg_engine, if_exists='replace', index=False)
                else:
                    df.to_sql(table, pg_engine, if_exists='replace', index=False, chunksize=5000)
                    print(f"  -> Successfully migrated {len(df)} rows for '{table}'.")
            except Exception as e:
                print(f"  -> ERROR migrating '{table}': {e}")
                continue

        # Enforce Primary Key
        pk_col = PRIMARY_KEYS.get(table)
        if pk_col:
            with pg_engine.begin() as pg_conn:
                try:
                    # Check if PK constraint already exists
                    pk_exists = pg_conn.execute(text(f"""
                        SELECT 1 FROM pg_constraint 
                        WHERE conrelid = '"{table}"'::regclass AND contype = 'p'
                    """)).scalar()
                    
                    if not pk_exists:
                        pg_conn.execute(text(f'ALTER TABLE "{table}" ADD PRIMARY KEY ("{pk_col}")'))
                        print(f"  -> Added primary key on '{pk_col}'.")
                except Exception as e:
                    print(f"  -> Warning: Could not add primary key to '{table}': {e}")

print("\n--- Recreating Triggers and Views ---")
with pg_engine.begin() as pg_conn:
    # 1. Trigger Function (BEFORE INSERT/UPDATE)
    pg_conn.execute(text("""
        CREATE OR REPLACE FUNCTION trg_customers_officer_before()
        RETURNS TRIGGER AS $$
        DECLARE
            v_staff_id VARCHAR(10);
            v_officer_type VARCHAR(50);
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.account_officer IS NOT NULL THEN
                    SELECT staff_id, officer_type
                    INTO v_staff_id, v_officer_type
                    FROM staff
                    WHERE TRIM(LOWER(NEW.account_officer)) = TRIM(LOWER(name_official))
                       OR TRIM(LOWER(NEW.account_officer)) = TRIM(LOWER(name_variant))
                    ORDER BY 
                        CASE 
                            WHEN TRIM(LOWER(NEW.account_officer)) = TRIM(LOWER(name_official)) THEN 1
                            ELSE 2
                        END
                    LIMIT 1;
                    
                    IF v_staff_id IS NOT NULL THEN
                        NEW.staff_id := v_staff_id;
                        NEW.officer_type := v_officer_type;
                    END IF;
                END IF;
                RETURN NEW;
            ELSIF TG_OP = 'UPDATE' THEN
                IF TRIM(LOWER(COALESCE(NEW.account_officer, ''))) <> TRIM(LOWER(COALESCE(OLD.account_officer, ''))) 
                   OR NEW.officer_type IS NULL 
                   OR NEW.officer_type = '' 
                   OR NEW.staff_id IS NULL THEN
                   
                    SELECT staff_id, officer_type
                    INTO v_staff_id, v_officer_type
                    FROM staff
                    WHERE TRIM(LOWER(NEW.account_officer)) = TRIM(LOWER(name_official))
                       OR TRIM(LOWER(NEW.account_officer)) = TRIM(LOWER(name_variant))
                    ORDER BY 
                        CASE 
                            WHEN TRIM(LOWER(NEW.account_officer)) = TRIM(LOWER(name_official)) THEN 1
                            ELSE 2
                        END
                    LIMIT 1;
                    
                    IF v_staff_id IS NOT NULL THEN
                        NEW.staff_id := v_staff_id;
                        NEW.officer_type := v_officer_type;
                    END IF;
                END IF;
                RETURN NEW;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """))
    
    # 2. Trigger Function (AFTER INSERT/UPDATE)
    pg_conn.execute(text("""
        CREATE OR REPLACE FUNCTION trg_customers_officer_after()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.staff_id IS NOT NULL THEN
                    INSERT INTO customer_officer_history
                        (account_number, staff_id, account_officer, officer_type, business_unit, start_date, is_current)
                    VALUES
                        (NEW.account_number, NEW.staff_id, NEW.account_officer, NEW.officer_type, NEW.business_unit, NOW(), 1);
                END IF;
                RETURN NEW;
            ELSIF TG_OP = 'UPDATE' THEN
                IF COALESCE(NEW.staff_id, '') <> COALESCE(OLD.staff_id, '') THEN
                    -- Close current history record
                    UPDATE customer_officer_history
                    SET end_date = NOW(),
                        is_current = 0
                    WHERE account_number = OLD.account_number
                      AND is_current = 1;
                      
                    -- Insert new history record
                    IF NEW.staff_id IS NOT NULL THEN
                        INSERT INTO customer_officer_history
                            (account_number, staff_id, account_officer, officer_type, business_unit, start_date, is_current)
                        VALUES
                            (NEW.account_number, NEW.staff_id, NEW.account_officer, NEW.officer_type, NEW.business_unit, NOW(), 1);
                    END IF;
                END IF;
                RETURN NEW;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """))

    # 3. Create Triggers (Drop first to allow re-running)
    pg_conn.execute(text("DROP TRIGGER IF EXISTS trg_customers_officer_before_insert ON customers"))
    pg_conn.execute(text("DROP TRIGGER IF EXISTS trg_customers_officer_before_update ON customers"))
    pg_conn.execute(text("DROP TRIGGER IF EXISTS trg_customers_officer_after_insert ON customers"))
    pg_conn.execute(text("DROP TRIGGER IF EXISTS trg_customers_officer_after_update ON customers"))
    
    pg_conn.execute(text("CREATE TRIGGER trg_customers_officer_before_insert BEFORE INSERT ON customers FOR EACH ROW EXECUTE FUNCTION trg_customers_officer_before()"))
    pg_conn.execute(text("CREATE TRIGGER trg_customers_officer_before_update BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION trg_customers_officer_before()"))
    pg_conn.execute(text("CREATE TRIGGER trg_customers_officer_after_insert AFTER INSERT ON customers FOR EACH ROW EXECUTE FUNCTION trg_customers_officer_after()"))
    pg_conn.execute(text("CREATE TRIGGER trg_customers_officer_after_update AFTER UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION trg_customers_officer_after()"))
    print("Created Triggers.")

    # 4. Create Views
    pg_conn.execute(text("""
        CREATE OR REPLACE VIEW all_payments AS
        SELECT account_number, amount_paid, date_of_payment, 'collection' AS payment_source FROM collections
        UNION ALL
        SELECT account_number, amount_paid, date_of_payment, COALESCE(payment_type, 'other') AS payment_source FROM other_payments
    """))
    
    pg_conn.execute(text("""
        CREATE OR REPLACE VIEW account_financial_summary AS
        SELECT 
            c.account_number,
            COALESCE(c.closing_balance, 0) AS closing_balance,
            COALESCE(p.total_payments, 0) AS total_payments,
            COALESCE(d.total_discounts, 0) AS total_discounts,
            COALESCE(a.total_adjustments, 0) AS total_adjustments,
            (((COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0)) - COALESCE(d.total_discounts, 0)) - COALESCE(a.total_adjustments, 0)) AS outstanding_balance,
            (CASE WHEN (COALESCE(p.total_payments, 0) >= (0.3 * COALESCE(c.closing_balance, 0))) THEN 'Yes' ELSE 'No' END) AS payment_plan
        FROM customers c
        LEFT JOIN (
            SELECT account_number, SUM(amount_paid) AS total_payments FROM all_payments GROUP BY account_number
        ) p ON c.account_number = p.account_number
        LEFT JOIN (
            SELECT account_number, SUM(discounted_amount) AS total_discounts FROM discounts 
            WHERE lower(status) = 'approved' OR lower(user_who_approved) LIKE '%okoye%' OR lower(user_who_approved) LIKE '%forstinus%' 
            GROUP BY account_number
        ) d ON c.account_number = d.account_number
        LEFT JOIN (
            SELECT account_number, SUM(adjustment_amount) AS total_adjustments FROM adjustments 
            WHERE lower(status) = 'approved' OR lower(user_who_approved_adjustment) LIKE '%okoye%' OR lower(user_who_approved_adjustment) LIKE '%forstinus%' 
            GROUP BY account_number
        ) a ON c.account_number = a.account_number
    """))
    print("Created Views.")

print("\nMigration Script Execution Completed Successfully!")
