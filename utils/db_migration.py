from sqlalchemy import text
import logging
import os
import time

# Configure logging to show progress in terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

def run_migrations(engine):
    """Entry point for all DB schema updates and data cleanup."""
    from db_utils import get_app_data_folder, get_local_mysql_engine, get_local_engine
    
    # 0. Singleton check
    lock_file = os.path.join(get_app_data_folder(), "migration.lock")
    if os.path.exists(lock_file):
        if time.time() - os.path.getmtime(lock_file) < 600:
            logging.info("Migration already in progress. Skipping.")
            return
    
    try:
        with open(lock_file, "w") as f:
            f.write(str(os.getpid()))
        
        # 1. RDS Migration (Main Cloud)
        try:
            _run_migrations_logic(engine, "RDS Cloud")
        except Exception as e:
            logging.warning(f"RDS Migration failed (likely offline): {e}")

        # 2. Local MySQL Workbench Migration (Backup)
        try:
            local_mysql = get_local_mysql_engine()
            _run_migrations_logic(local_mysql, "Local MySQL Workbench")
        except Exception as e:
            logging.warning(f"Local MySQL Migration failed: {e}")

        # 3. SQLite Local Cache Migration (Offline Mode)
        try:
            local_sqlite = get_local_engine()
            _run_migrations_sqlite(local_sqlite)
        except Exception as e:
            logging.error(f"SQLite Migration failed: {e}")

    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)

def _run_migrations_sqlite(local_engine):
    """Initializes the local SQLite database used for offline caching."""
    with local_engine.begin() as conn:
        # staff table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staff (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username VARCHAR(50) UNIQUE,
                password_hash TEXT,
                first_name VARCHAR(100),
                middle_name VARCHAR(100),
                surname VARCHAR(100),
                full_name VARCHAR(200),
                name_official VARCHAR(200),
                name_variant VARCHAR(200),
                officer_type VARCHAR(50),
                business_unit VARCHAR(100),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                staff_id VARCHAR(50),
                email VARCHAR(100),
                phone_number VARCHAR(50),
                role VARCHAR(20),
                status VARCHAR(20) DEFAULT 'Active',
                last_online_login TIMESTAMP,
                transaction_id VARCHAR(100),
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """))

        # user_activity_log
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username VARCHAR(50),
                action VARCHAR(100),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                details TEXT,
                session_id VARCHAR(100),
                tab_id VARCHAR(50),
                event_type VARCHAR(20) DEFAULT 'MINOR',
                transaction_id VARCHAR(100),
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """))

        # customers (Last known state)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number VARCHAR(50) UNIQUE,
                account_name VARCHAR(200),
                business_unit VARCHAR(100),
                account_officer VARCHAR(100),
                phone_number VARCHAR(50),
                account_type VARCHAR(50),
                closing_balance DECIMAL(15, 2) DEFAULT 0.00,
                undertaking VARCHAR(100),
                dt_name VARCHAR(100),
                feeder VARCHAR(100),
                batch VARCHAR(50),
                customer_status VARCHAR(50),
                last_pay_amt_pre_deactivation DECIMAL(15, 2),
                last_pay_date_pre_deactivation DATE,
                account_address TEXT,
                address TEXT,
                sync_status VARCHAR(20) DEFAULT 'SYNCED'
            );
        """))

        # collections
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS collections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address TEXT,
                date_of_payment DATETIME,
                debt_balance_last_payment DECIMAL(15, 2),
                amount_paid DECIMAL(15, 2),
                current_balance DECIMAL(15, 2),
                transaction_id VARCHAR(100) UNIQUE,
                receipt_number VARCHAR(100),
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                business_unit VARCHAR(100),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """))

        # other_payments
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS other_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number VARCHAR(50),
                amount_paid DECIMAL(15, 2),
                date_of_payment DATETIME,
                justification TEXT,
                payment_type VARCHAR(100),
                dedup_hash VARCHAR(64),
                transaction_id VARCHAR(100) UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """))

        # validation
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS validation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                validation_date DATETIME,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address TEXT,
                phone_number VARCHAR(50),
                customer_email VARCHAR(255),
                physical_status VARCHAR(100),
                picture_1 VARCHAR(255),
                picture_2 VARCHAR(255),
                picture_3 VARCHAR(255),
                gps_coordinate VARCHAR(100),
                reason_for_non_payment TEXT,
                dmo VARCHAR(100),
                dms VARCHAR(100),
                action_required TEXT,
                source VARCHAR(50) DEFAULT 'App',
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """))
        
        # disconnections
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS disconnections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                disconnection_date DATETIME,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address VARCHAR(255),
                dmo VARCHAR(100),
                dms VARCHAR(100),
                dt_name VARCHAR(100),
                ut_name VARCHAR(100),
                bu_name VARCHAR(100),
                picture_of_disconnection VARCHAR(500),
                picture_of_premises VARCHAR(500),
                longitude DECIMAL(11,8),
                latitude DECIMAL(10,8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """))

        # resolutions
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS resolutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address TEXT,
                resolution TEXT,
                outcome_of_actions_taken TEXT,
                new_resolution TEXT,
                abandoned_duplicate_account_no VARCHAR(100),
                ppm_no VARCHAR(100),
                sr_number VARCHAR(100),
                reference_details VARCHAR(255),
                resolved_by VARCHAR(150),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """))

        # discounts
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS discounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                percentage_discount DECIMAL(5, 2),
                discounted_amount DECIMAL(15, 2),
                total_debt_at_migration DECIMAL(15, 2),
                total_debt_after_discount DECIMAL(15, 2),
                status VARCHAR(50),
                date_applied DATETIME,
                date_approved DATETIME,
                user_who_raised VARCHAR(150),
                user_who_approved VARCHAR(150),
                business_unit VARCHAR(100),
                undertaking VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                sync_status VARCHAR(20) DEFAULT 'PENDING',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

        # adjustments
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                adjustment_amount DECIMAL(15, 2),
                total_debt_at_migration DECIMAL(15, 2),
                total_debt_after_adjustment DECIMAL(15, 2),
                status VARCHAR(50),
                date_applied DATETIME,
                date_approved DATETIME,
                user_who_raised_adjustment VARCHAR(255),
                user_who_approved_adjustment VARCHAR(255),
                business_unit VARCHAR(100),
                undertaking VARCHAR(100),
                remark TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                sync_status VARCHAR(20) DEFAULT 'PENDING',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

        # performance_config
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS performance_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bu_name VARCHAR(100) UNIQUE,
                monthly_target DECIMAL(15, 2) DEFAULT 10000000.00,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

        # --- SQLite Schema Upgrades (Ensuring columns exist in existing tables) ---
        tables_to_upgrade = {
            'user_activity_log': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('session_id', 'VARCHAR(100)'),
                ('tab_id', 'VARCHAR(50)'),
                ('event_type', "VARCHAR(20) DEFAULT 'MINOR'")
            ],
            'collections': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('business_unit', 'VARCHAR(100)'),
                ('account_name', 'VARCHAR(255)'),
                ('account_address', 'TEXT'),
                ('date_of_payment', 'DATETIME'),
                ('debt_balance_last_payment', 'DECIMAL(15, 2)'),
                ('amount_paid', 'DECIMAL(15, 2)'),
                ('current_balance', 'DECIMAL(15, 2)'),
                ('receipt_number', 'VARCHAR(100)'),
                ('import_date', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ],
            'validation': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('customer_email', 'VARCHAR(255)'),
                ('picture_1', 'VARCHAR(255)'),
                ('picture_2', 'VARCHAR(255)'),
                ('picture_3', 'VARCHAR(255)'),
                ('import_date', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('source', "VARCHAR(50) DEFAULT 'App'")
            ],
            'disconnections': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ],
            'resolutions': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('account_name', 'VARCHAR(255)'),
                ('account_address', 'TEXT'),
                ('new_resolution', 'TEXT'),
                ('abandoned_duplicate_account_no', 'VARCHAR(100)'),
                ('ppm_no', 'VARCHAR(100)'),
                ('sr_number', 'VARCHAR(100)'),
                ('reference_details', 'VARCHAR(255)'),
                ('resolved_by', 'VARCHAR(150)'),
                ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ],
            'other_payments': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('justification', 'TEXT')
            ],
            'staff': [
                ('status', "VARCHAR(20) DEFAULT 'Active'"),
                ('staff_id', 'VARCHAR(50)'),
                ('email', 'VARCHAR(100)'),
                ('phone_number', 'VARCHAR(50)'),
                ('role', 'VARCHAR(20)'),
                ('last_online_login', 'TIMESTAMP NULL'),
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'")
            ],
            'discounts': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('account_name', 'VARCHAR(255)'),
                ('percentage_discount', 'DECIMAL(5, 2)'),
                ('total_debt_at_migration', 'DECIMAL(15, 2)'),
                ('total_debt_after_discount', 'DECIMAL(15, 2)'),
                ('date_approved', 'DATETIME'),
                ('user_who_raised', 'VARCHAR(150)'),
                ('business_unit', 'VARCHAR(100)'),
                ('undertaking', 'VARCHAR(100)'),
                ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ],
            'adjustments': [
                ('transaction_id', 'VARCHAR(100)'),
                ('sync_status', "VARCHAR(20) DEFAULT 'PENDING'"),
                ('updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ('account_name', 'VARCHAR(255)'),
                ('total_debt_at_migration', 'DECIMAL(15, 2)'),
                ('total_debt_after_adjustment', 'DECIMAL(15, 2)'),
                ('date_approved', 'DATETIME'),
                ('user_who_raised_adjustment', 'VARCHAR(255)'),
                ('business_unit', 'VARCHAR(100)'),
                ('undertaking', 'VARCHAR(100)'),
                ('remark', 'TEXT'),
                ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ]
        }

        # Special check for customers table in SQLite: Cannot add PK to existing table
        cursor = conn.execute(text("PRAGMA table_info(customers)"))
        cust_cols = [row[1] for row in cursor.fetchall()]
        if 'id' not in cust_cols:
            try:
                logging.info("Migrating local 'customers' table to include 'id' column...")
                # 1. Rename existing
                conn.execute(text("ALTER TABLE customers RENAME TO customers_old"))
                # 2. Create new with ID
                conn.execute(text("""
                    CREATE TABLE customers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_number VARCHAR(50) UNIQUE,
                        account_name VARCHAR(200),
                        business_unit VARCHAR(100),
                        account_officer VARCHAR(100),
                        phone_number VARCHAR(50),
                        account_type VARCHAR(50),
                        closing_balance DECIMAL(15, 2) DEFAULT 0.00,
                        undertaking VARCHAR(100),
                        dt_name VARCHAR(100),
                        feeder VARCHAR(100),
                        batch VARCHAR(50),
                        customer_status VARCHAR(50),
                        last_pay_amt_pre_deactivation DECIMAL(15, 2),
                        last_pay_date_pre_deactivation DATE,
                        account_address TEXT,
                        address TEXT,
                        sync_status VARCHAR(20) DEFAULT 'SYNCED'
                    );
                """))
                # 3. Copy data (id will auto-populate)
                conn.execute(text("""
                    INSERT INTO customers (
                        account_number, account_name, business_unit, account_officer, 
                        phone_number, account_type, closing_balance, undertaking, 
                        dt_name, feeder, batch, customer_status, 
                        last_pay_amt_pre_deactivation, last_pay_date_pre_deactivation, 
                        account_address, address, sync_status
                    ) 
                    SELECT 
                        account_number, account_name, business_unit, account_officer, 
                        phone_number, account_type, closing_balance, undertaking, 
                        dt_name, feeder, batch, customer_status, 
                        last_pay_amt_pre_deactivation, last_pay_date_pre_deactivation, 
                        account_address, address, sync_status 
                    FROM customers_old
                """))
                # 4. Drop old
                conn.execute(text("DROP TABLE customers_old"))
                logging.info("Successfully added 'id' column to local 'customers' table.")
            except Exception as e:
                logging.error(f"Failed to migrate local 'customers' table: {e}")
                # Try to restore if failed
                try: conn.execute(text("ALTER TABLE customers_old RENAME TO customers"))
                except: pass

        for table, new_cols in tables_to_upgrade.items():
            # SQLite specific column check
            cursor = conn.execute(text(f"PRAGMA table_info({table})"))
            existing_cols = [row[1] for row in cursor.fetchall()]
            for col_name, col_def in new_cols:
                if col_name not in existing_cols:
                    try:
                        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}"))
                        logging.info(f"Added missing column '{col_name}' to local SQLite table '{table}'.")
                    except Exception as e:
                        logging.error(f"Failed to migrate local SQLite table '{table}' column '{col_name}': {e}")

        # all_payments view (for SQLite local cache)
        conn.execute(text("DROP VIEW IF EXISTS all_payments;"))
        conn.execute(text("""
            CREATE VIEW all_payments AS
            SELECT 
                account_number, 
                amount_paid, 
                date_of_payment, 
                'collection' as payment_source 
            FROM collections
            UNION ALL
            SELECT 
                account_number, 
                amount_paid, 
                date_of_payment, 
                COALESCE(payment_type, 'other') as payment_source 
            FROM other_payments;
        """))

        # account_financial_summary view (for SQLite local cache)
        conn.execute(text("DROP VIEW IF EXISTS account_financial_summary;"))
        conn.execute(text("""
            CREATE VIEW account_financial_summary AS
            SELECT 
                c.account_number,
                COALESCE(p.total_payments, 0) as total_payments,
                COALESCE(d.total_discounts, 0) as total_discounts,
                COALESCE(a.total_adjustments, 0) as total_adjustments,
                (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) + COALESCE(a.total_adjustments, 0)) as outstanding_balance,
                CASE 
                    WHEN (COALESCE(p.total_payments, 0) >= 0.3 * COALESCE(c.closing_balance, 0)) 
                    AND (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) + COALESCE(a.total_adjustments, 0) > 0) 
                    THEN 'Yes' ELSE 'No' 
                END as payment_plan
            FROM customers c
            LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments GROUP BY account_number) p ON c.account_number = p.account_number
            LEFT JOIN (SELECT account_number, SUM(discounted_amount) as total_discounts FROM discounts WHERE status = 'approved' GROUP BY account_number) d ON c.account_number = d.account_number
            LEFT JOIN (SELECT account_number, SUM(adjustment_amount) as total_adjustments FROM adjustments WHERE status = 'approved' GROUP BY account_number) a ON c.account_number = a.account_number;
        """))
        
        # Ensure temp outbox tables exist 
        tables_for_outbox = ['collections', 'other_payments', 'validation', 'disconnections', 'resolutions', 'discounts', 'adjustments']
        for t in tables_for_outbox:
            try:
                # We do not use CREATE TABLE AS SELECT because it drops UNIQUE constraints
                # So we simply mirror the existing table schema by dynamically fetching it.
                res = conn.execute(text(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{t}'")).fetchone()
                if res and res[0]:
                    temp_sql = res[0].replace(f"CREATE TABLE {t}", f"CREATE TABLE IF NOT EXISTS temp_{t}", 1).replace(f'CREATE TABLE "{t}"', f'CREATE TABLE IF NOT EXISTS "temp_{t}"', 1)
                    conn.execute(text(temp_sql))
                    logging.info(f"Verified outbox staging table temp_{t}.")
            except Exception as e:
                logging.error(f"Failed to create temp outbox table for {t}: {e}")

        # Default Offline Admin Check (Allows login on very first .exe execution without internet)
        res = conn.execute(text("SELECT 1 FROM staff LIMIT 1")).fetchone()
        if not res:
            conn.execute(text("""
                INSERT INTO staff (username, password_hash, full_name, role) 
                VALUES ('admin', 'admin', 'Default Offline Administrator', 'Admin')
            """))

        logging.info("SQLite local schema initialization completed successfully.")

def _run_migrations_logic(engine, db_name):
    """Standardized migration logic for MySQL-based engines (RDS and Local MySQL)."""
    logging.info(f"Starting migrations for {db_name}...")
    
    def is_applied(name):
        with engine.connect() as conn:
            res = conn.execute(text("SELECT 1 FROM migration_meta WHERE migration_name = :n"), {"n": name}).fetchone()
            return res is not None

    def mark_applied(name):
        with engine.begin() as conn:
            conn.execute(text("INSERT IGNORE INTO migration_meta (migration_name) VALUES (:n)"), {"n": name})

    # 1. Ensure Core Tables Exist (Fresh Install / Migration)
    # We use engine.begin() which handles transaction and ensures we are connected to the DB
    with engine.begin() as conn:
        # migration_meta
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS migration_meta (
                migration_name VARCHAR(100) PRIMARY KEY,
                applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """))

        # staff
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staff (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE,
                password_hash TEXT,
                first_name VARCHAR(100),
                middle_name VARCHAR(100),
                surname VARCHAR(100),
                full_name VARCHAR(200),
                name_official VARCHAR(200),
                name_variant VARCHAR(200),
                officer_type VARCHAR(50),
                business_unit VARCHAR(100),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                staff_id VARCHAR(50),
                email VARCHAR(100),
                phone_number VARCHAR(50),
                role VARCHAR(20),
                status VARCHAR(20) DEFAULT 'Active',
                last_online_login DATETIME,
                transaction_id VARCHAR(100),
                sync_status VARCHAR(20) DEFAULT 'SYNCED'
            ) ENGINE=InnoDB;
        """))

        # user_activity_log
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_activity_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50),
                action VARCHAR(100),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                details TEXT,
                session_id VARCHAR(100),
                tab_id VARCHAR(50),
                event_type VARCHAR(20) DEFAULT 'MINOR',
                transaction_id VARCHAR(100),
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            ) ENGINE=InnoDB;
        """))

        # customers
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customers (
                id INT AUTO_INCREMENT UNIQUE,
                account_number VARCHAR(50) PRIMARY KEY,
                account_name VARCHAR(200),
                business_unit VARCHAR(100),
                account_officer VARCHAR(100),
                phone_number VARCHAR(50),
                account_type VARCHAR(50),
                closing_balance DECIMAL(15, 2) DEFAULT 0.00,
                undertaking VARCHAR(100),
                dt_name VARCHAR(100),
                feeder VARCHAR(100),
                batch VARCHAR(50),
                customer_status VARCHAR(50),
                last_pay_amt_pre_deactivation DECIMAL(15, 2),
                last_pay_date_pre_deactivation DATE,
                account_address TEXT,
                address TEXT
            ) ENGINE=InnoDB;
        """))

        # collections
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS collections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address TEXT,
                date_of_payment DATETIME,
                debt_balance_last_payment DECIMAL(15, 2),
                amount_paid DECIMAL(15, 2),
                current_balance DECIMAL(15, 2),
                transaction_id VARCHAR(100) UNIQUE,
                receipt_number VARCHAR(100),
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                business_unit VARCHAR(100),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            ) ENGINE=InnoDB;
        """))

        # validation
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS validation (
                id INT AUTO_INCREMENT PRIMARY KEY,
                validation_date DATETIME,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address TEXT,
                phone_number VARCHAR(50),
                customer_email VARCHAR(255),
                physical_status VARCHAR(100),
                picture_1 VARCHAR(255),
                picture_2 VARCHAR(255),
                picture_3 VARCHAR(255),
                gps_coordinate VARCHAR(100),
                reason_for_non_payment TEXT,
                dmo VARCHAR(100),
                dms VARCHAR(100),
                action_required TEXT,
                source VARCHAR(50) DEFAULT 'App',
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            ) ENGINE=InnoDB;
        """))

        # disconnections
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS disconnections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                disconnection_date DATETIME,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address VARCHAR(255),
                dmo VARCHAR(100),
                dms VARCHAR(100),
                dt_name VARCHAR(100),
                ut_name VARCHAR(100),
                bu_name VARCHAR(100),
                picture_of_disconnection VARCHAR(500),
                picture_of_premises VARCHAR(500),
                longitude DECIMAL(11,8),
                latitude DECIMAL(10,8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'PENDING'
            ) ENGINE=InnoDB;
        """))

        # resolutions
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS resolutions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                account_address TEXT,
                resolution TEXT,
                outcome_of_actions_taken TEXT,
                new_resolution TEXT,
                abandoned_duplicate_account_no VARCHAR(100),
                ppm_no VARCHAR(100),
                sr_number VARCHAR(100),
                reference_details VARCHAR(255),
                resolved_by VARCHAR(150),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                sync_status VARCHAR(20) DEFAULT 'PENDING',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """))

        # discounts
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS discounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                percentage_discount DECIMAL(5, 2),
                discounted_amount DECIMAL(15, 2),
                total_debt_at_migration DECIMAL(15, 2),
                total_debt_after_discount DECIMAL(15, 2),
                status VARCHAR(50),
                date_applied DATETIME,
                date_approved DATETIME,
                user_who_raised VARCHAR(150),
                user_who_approved VARCHAR(150),
                business_unit VARCHAR(100),
                undertaking VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                sync_status VARCHAR(20) DEFAULT 'PENDING',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """))

        # adjustments
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS adjustments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_number VARCHAR(50),
                account_name VARCHAR(255),
                adjustment_amount DECIMAL(15, 2),
                total_debt_at_migration DECIMAL(15, 2),
                total_debt_after_adjustment DECIMAL(15, 2),
                status VARCHAR(50),
                date_applied DATETIME,
                date_approved DATETIME,
                user_who_raised_adjustment VARCHAR(255),
                user_who_approved_adjustment VARCHAR(255),
                business_unit VARCHAR(100),
                undertaking VARCHAR(100),
                remark TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_id VARCHAR(100) UNIQUE,
                sync_status VARCHAR(20) DEFAULT 'PENDING',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """))

    # Create Tables (Core Logic)
    with engine.begin() as conn:
        # customers
        res = conn.execute(text("SHOW COLUMNS FROM customers LIKE 'id'")).fetchone()
        if not res:
            try:
                # Add id as an auto-incrementing column (this is MySQL specific)
                conn.execute(text("ALTER TABLE customers ADD COLUMN id INT AUTO_INCREMENT UNIQUE"))
                logging.info("Added auto-increment 'id' column to 'customers' table.")
            except Exception as e:
                logging.error(f"Failed to add 'id' column to 'customers': {e}")

        # other_payments
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS other_payments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_number VARCHAR(50),
                amount_paid DECIMAL(15, 2),
                date_of_payment DATE,
                payment_type VARCHAR(100),
                reference VARCHAR(100),
                dedup_hash VARCHAR(64)
            ) ENGINE=InnoDB;
        """))

        # accounts (Legacy structure)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_number VARCHAR(50) UNIQUE,
                business_unit VARCHAR(100),
                account_officer VARCHAR(100)
            ) ENGINE=InnoDB;
        """))

        # customer_officer_history
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customer_officer_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_number VARCHAR(50),
                account_officer VARCHAR(100),
                officer_type VARCHAR(50),
                start_date DATE,
                end_date DATE,
                INDEX (account_number),
                INDEX (start_date, end_date)
            ) ENGINE=InnoDB;
        """))

    # Create Views (Dependent on Tables)
    if not is_applied(f"sync_views_v3_{db_name.replace(' ', '_')}"):
        try:
            with engine.begin() as conn:
                # account_financial_summary view
                conn.execute(text("""
                    CREATE OR REPLACE VIEW account_financial_summary AS
                    SELECT 
                        c.account_number,
                        COALESCE(p.total_payments, 0) as total_payments,
                        COALESCE(d.total_discounts, 0) as total_discounts,
                        COALESCE(a.total_adjustments, 0) as total_adjustments,
                        (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) - COALESCE(a.total_adjustments, 0)) as outstanding_balance,
                        CASE 
                            WHEN (COALESCE(p.total_payments, 0) >= 0.3 * COALESCE(c.closing_balance, 0)) 
                            AND (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) - COALESCE(a.total_adjustments, 0) > 0) 
                            THEN 'Yes' ELSE 'No' 
                        END as payment_plan
                    FROM customers c
                    LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments GROUP BY account_number) p ON c.account_number = p.account_number
                    LEFT JOIN (SELECT account_number, SUM(discounted_amount) as total_discounts FROM discounts WHERE LOWER(status) = 'approved' OR LOWER(user_who_approved) LIKE '%okoye%' OR LOWER(user_who_approved) LIKE '%forstinus%' GROUP BY account_number) d ON c.account_number = d.account_number
                    LEFT JOIN (SELECT account_number, SUM(adjustment_amount) as total_adjustments FROM adjustments WHERE LOWER(status) = 'approved' OR LOWER(user_who_approved_adjustment) LIKE '%okoye%' OR LOWER(user_who_approved_adjustment) LIKE '%forstinus%' GROUP BY account_number) a ON c.account_number = a.account_number;
                """))
                logging.info(f"Created/Updated account_financial_summary view for {db_name}.")
                
            mark_applied(f"sync_views_v3_{db_name.replace(' ', '_')}")
        except Exception as e:
            logging.error(f"Failed to sync views for {db_name}: {e}")

    # Default Admin Check
    with engine.begin() as conn:
        res = conn.execute(text("SELECT 1 FROM staff LIMIT 1")).fetchone()
        if not res:
            # Add a default admin user if table is empty
            # Note: password is 'admin' (plain text for initial migration, auth_service handles hash upgrade)
            conn.execute(text("""
                INSERT INTO staff (username, password_hash, full_name, role) 
                VALUES ('admin', 'admin', 'Default Administrator', 'Admin')
            """))
            logging.info("Created default 'admin' user with password 'admin'. Please change it immediately.")

    # Ensure transaction_id and updated_at exist on core tables for Transactional Sync
    # This now runs for BOTH RDS and Local MySQL
    # Version 2: Added business_unit check for collections
    if is_applied(f"sync_schema_v2_{db_name.replace(' ', '_')}"):
        return

    try:
        with engine.begin() as conn:
            tables_to_update = ['staff', 'collections', 'other_payments', 'validation', 'user_activity_log', 'disconnections', 'resolutions', 'discounts', 'adjustments']
            for table in tables_to_update:
                # Standard MySQL column check
                cols = conn.execute(text(f"SHOW COLUMNS FROM {table}")).fetchall()
                col_names = [c[0].lower() for c in cols]
                
                if 'sync_status' not in col_names:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN sync_status VARCHAR(20) DEFAULT 'SYNCED'"))
                if 'transaction_id' not in col_names:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN transaction_id VARCHAR(100)"))
                    conn.execute(text(f"ALTER TABLE {table} ADD UNIQUE INDEX (transaction_id)"))
                if 'updated_at' not in col_names:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
                
                # Special check for staff table: last_online_login
                if table == 'staff' and 'last_online_login' not in col_names:
                    conn.execute(text("ALTER TABLE staff ADD COLUMN last_online_login DATETIME NULL"))
                    logging.info("Added 'last_online_login' column to existing staff table.")

                # Special check for collections table: business_unit (Required for proper reporting)
                if table == 'collections' and 'business_unit' not in col_names:
                    conn.execute(text("ALTER TABLE collections ADD COLUMN business_unit VARCHAR(100) NULL"))
                    logging.info("Added 'business_unit' column to existing collections table.")
        
        # Mark as applied so it doesn't run every time
        mark_applied(f"sync_schema_v2_{db_name.replace(' ', '_')}")
        logging.info(f"Synchronized schema for {db_name} (Transactional Columns).")
    except Exception as e:
        logging.error(f"Failed to sync transactional columns for {db_name}: {e}")

    # Ensure UNIQUE indexes for Deduplication natively on MySQL (Cloud RDS)
    # This is critical because INSERT IGNORE requires physical UNIQUE indexes to drop duplicates!
    if not is_applied(f"sync_schema_v3_{db_name.replace(' ', '_')}"):
        try:
            with engine.begin() as conn:
                tables_to_update = ['staff', 'collections', 'other_payments', 'validation', 'user_activity_log', 'disconnections', 'resolutions', 'discounts', 'adjustments']
                for table in tables_to_update:
                    res = conn.execute(text(f"SHOW INDEX FROM {table} WHERE Key_name = 'transaction_id'"))
                    if not res.fetchone():
                        try:
                            conn.execute(text(f"ALTER TABLE {table} ADD UNIQUE INDEX (transaction_id)"))
                            logging.info(f"Enforced physical UNIQUE INDEX on {table}.transaction_id in {db_name} for duplicate prevention.")
                        except Exception as e:
                            logging.warning(f"Could not enforce UNIQUE INDEX on {table} in {db_name} (It might already contain duplicate data physically). Error: {e}")
                            # If it fails, the AWS DBA needs to delete the existing duplicates natively first or ignore.
            mark_applied(f"sync_schema_v3_{db_name.replace(' ', '_')}")
        except Exception as e:
            logging.error(f"Failed to apply v3 constraint sync for {db_name}: {e}")

    # Enforce Legacy Schema Complete Column Parity (v4)
    if not is_applied(f"sync_schema_v4_{db_name.replace(' ', '_')}"):
        try:
            with engine.begin() as conn:
                legacy_columns = {
                    'collections': [
                        ('account_name', 'VARCHAR(255)'),
                        ('account_address', 'TEXT'),
                        ('date_of_payment', 'DATETIME'),
                        ('debt_balance_last_payment', 'DECIMAL(15, 2)'),
                        ('amount_paid', 'DECIMAL(15, 2)'),
                        ('current_balance', 'DECIMAL(15, 2)'),
                        ('receipt_number', 'VARCHAR(100)'),
                        ('import_date', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    ],
                    'validation': [
                        ('customer_email', 'VARCHAR(255)'),
                        ('picture_1', 'VARCHAR(255)'),
                        ('picture_2', 'VARCHAR(255)'),
                        ('picture_3', 'VARCHAR(255)'),
                        ('import_date', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                        ('source', "VARCHAR(50) DEFAULT 'App'")
                    ],
                    'disconnections': [
                        ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    ],
                    'resolutions': [
                        ('account_name', 'VARCHAR(255)'),
                        ('account_address', 'TEXT'),
                        ('new_resolution', 'TEXT'),
                        ('abandoned_duplicate_account_no', 'VARCHAR(100)'),
                        ('ppm_no', 'VARCHAR(100)'),
                        ('sr_number', 'VARCHAR(100)'),
                        ('reference_details', 'VARCHAR(255)'),
                        ('resolved_by', 'VARCHAR(150)'),
                        ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    ],
                    'other_payments': [
                        ('justification', 'TEXT')
                    ],
                    'discounts': [
                        ('account_name', 'VARCHAR(255)'),
                        ('percentage_discount', 'DECIMAL(5, 2)'),
                        ('total_debt_at_migration', 'DECIMAL(15, 2)'),
                        ('total_debt_after_discount', 'DECIMAL(15, 2)'),
                        ('date_approved', 'DATETIME'),
                        ('user_who_raised', 'VARCHAR(150)'),
                        ('business_unit', 'VARCHAR(100)'),
                        ('undertaking', 'VARCHAR(100)'),
                        ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    ],
                    'adjustments': [
                        ('account_name', 'VARCHAR(255)'),
                        ('total_debt_at_migration', 'DECIMAL(15, 2)'),
                        ('total_debt_after_adjustment', 'DECIMAL(15, 2)'),
                        ('date_approved', 'DATETIME'),
                        ('user_who_raised_adjustment', 'VARCHAR(255)'),
                        ('business_unit', 'VARCHAR(100)'),
                        ('undertaking', 'VARCHAR(100)'),
                        ('remark', 'TEXT'),
                        ('created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    ]
                }
                
                for table, cols_to_add in legacy_columns.items():
                    try:
                        existing_cols_query = conn.execute(text(f"SHOW COLUMNS FROM {table}")).fetchall()
                        existing_col_names = [c[0].lower() for c in existing_cols_query]
                        
                        for col_name, col_type in cols_to_add:
                            if col_name.lower() not in existing_col_names:
                                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"))
                                logging.info(f"Added legacy column {col_name} to {table} in {db_name}")
                    except Exception as loop_e:
                        logging.error(f"Error checking legacy columns for {table}: {loop_e}")
                        
            mark_applied(f"sync_schema_v4_{db_name.replace(' ', '_')}")
            logging.info(f"Successfully applied v4 legacy schema parity to {db_name}")
        except Exception as e:
            logging.error(f"Failed to apply v4 legacy update for {db_name}: {e}")

    # 2. Audit Log Table (Core Infrastructure)
    if not is_applied("create_audit_log_v2"):
        try:
            with engine.begin() as conn:
                # Add session_id and tab_id to user_activity_log if they don't exist
                # Using a fresh table if necessary or ALTER
                # Check for existing columns to support upgrade from v1
                cols = conn.execute(text("SHOW COLUMNS FROM user_activity_log")).fetchall()
                col_names = [c[0] for c in cols]
                
                if 'session_id' not in col_names:
                    conn.execute(text("ALTER TABLE user_activity_log ADD COLUMN session_id VARCHAR(100)"))
                if 'tab_id' not in col_names:
                    conn.execute(text("ALTER TABLE user_activity_log ADD COLUMN tab_id VARCHAR(50)"))
                    
            mark_applied("create_audit_log_v2")
            logging.info("Upgraded user_activity_log table to v2.")
        except Exception as e:
            logging.error(f"Failed to upgrade audit log: {e}")

    # 2.1 Audit Log Table v3 (Event Classification)
    if not is_applied("create_audit_log_v3"):
        try:
            with engine.begin() as conn:
                # Add event_type column to user_activity_log if it doesn't exist
                cols = conn.execute(text("SHOW COLUMNS FROM user_activity_log")).fetchall()
                col_names = [c[0] for c in cols]
                
                if 'event_type' not in col_names:
                    conn.execute(text("ALTER TABLE user_activity_log ADD COLUMN event_type VARCHAR(20) DEFAULT 'MINOR' AFTER details"))
                    
            mark_applied("create_audit_log_v3")
            logging.info("Upgraded user_activity_log table to v3 (Event Classification).")
        except Exception as e:
            logging.error(f"Failed to upgrade audit log to v3: {e}")

    # 3. all_payments (Legacy Sync - No longer needed as all_payments is a VIEW)
    # The view definition above handles all necessary columns.
    mark_applied("all_payments_payment_source")

    # 3.2 other_payments schema sync
    if not is_applied("other_payments_schema_sync"):
        try:
            with engine.begin() as conn:
                cols = conn.execute(text("SHOW COLUMNS FROM other_payments")).fetchall()
                col_names = [c[0] for c in cols]
                if 'dedup_hash' not in col_names:
                    conn.execute(text("ALTER TABLE other_payments ADD COLUMN dedup_hash VARCHAR(64)"))
                if 'payment_type' not in col_names:
                    conn.execute(text("ALTER TABLE other_payments ADD COLUMN payment_type VARCHAR(100)"))
            mark_applied("other_payments_schema_sync")
            logging.info("Ensured dedup_hash and payment_type exist on other_payments.")
        except Exception as e:
            logging.error(f"Failed to sync other_payments schema: {e}")

    # Ensure staff table has status column
    if not is_applied("staff_status_column"):
        try:
            with engine.begin() as conn:
                cols = conn.execute(text("SHOW COLUMNS FROM staff")).fetchall()
                col_names = [c[0] for c in cols]
                if 'status' not in col_names:
                    conn.execute(text("ALTER TABLE staff ADD COLUMN status VARCHAR(20) DEFAULT 'Active'"))
            mark_applied("staff_status_column")
            logging.info("Ensured status column exists on staff table in RDS.")
        except Exception as e:
            logging.error(f"Failed to add status column to staff table: {e}")

    if not is_applied("customers_dashboard_columns"):
        try:
            with engine.begin() as conn:
                cols = conn.execute(text("SHOW COLUMNS FROM customers")).fetchall()
                col_names = [c[0] for c in cols]
                
                new_cols = [
                    ('feeder', 'VARCHAR(100)'),
                    ('batch', 'VARCHAR(50)'),
                    ('customer_status', 'VARCHAR(50)'),
                    ('account_address', 'TEXT')
                ]
                
                for col_name, col_type in new_cols:
                    if col_name not in col_names:
                        conn.execute(text(f"ALTER TABLE customers ADD COLUMN {col_name} {col_type}"))
                        
            mark_applied("customers_dashboard_columns")
            logging.info("Ensured dashboard columns exist on customers.")
        except Exception as e:
            logging.error(f"Failed to sync customers dashboard columns: {e}")

    # 4. Performance Metrics Config
    if not is_applied("create_performance_config"):
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS performance_config (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        bu_name VARCHAR(100) UNIQUE,
                        monthly_target DECIMAL(15, 2) DEFAULT 10000000.00,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                """))
                
                # Populate default targets
                bus = ["ABULE-EGBA", "AKOWONJO", "IKEJA", "IKORODU", "OSHODI", "SHOMOLU"]
                for bu in bus:
                    conn.execute(text("INSERT IGNORE INTO performance_config (bu_name, monthly_target) VALUES (:bu, 10000000.00)"), {"bu": bu})
                
                # Special target for MD
                conn.execute(text("INSERT IGNORE INTO performance_config (bu_name, monthly_target) VALUES ('MD', 8500000.00)"))
                
            mark_applied("create_performance_config")
            logging.info("Created performance_config table and populated targets.")
        except Exception as e:
            logging.error(f"Failed to create performance config: {e}")

    table_configs = [
        {
            "name": "collections",
            "unique_keys": [
                ("uniq_transaction_id", "transaction_id")
            ],
            "cleanup": [
                "DELETE t1 FROM collections t1 INNER JOIN collections t2 ON t1.transaction_id = t2.transaction_id WHERE t1.id < t2.id AND t1.transaction_id IS NOT NULL AND t1.transaction_id != ''",
                "DELETE t1 FROM collections t1 INNER JOIN collections t2 ON t1.account_number = t2.account_number AND t1.amount_paid = t2.amount_paid AND t1.date_of_payment = t2.date_of_payment WHERE t1.id < t2.id AND (t1.transaction_id IS NULL OR t1.transaction_id = '')"
            ]
        },
        {
            "name": "other_payments",
            "unique_keys": [
                ("uniq_other_hash", "dedup_hash")
            ],
            "cleanup": [
                "DELETE t1 FROM other_payments t1 INNER JOIN other_payments t2 ON t1.dedup_hash = t2.dedup_hash WHERE t1.id < t2.id AND t1.dedup_hash IS NOT NULL AND t1.dedup_hash != ''",
                "DELETE t1 FROM other_payments t1 INNER JOIN other_payments t2 ON t1.account_number = t2.account_number AND t1.amount_paid = t2.amount_paid AND t1.date_of_payment = t2.date_of_payment WHERE t1.id < t2.id AND (t1.dedup_hash IS NULL OR t1.dedup_hash = '')"
            ]
        },
        {
            "name": "validation",
            "unique_keys": [("uniq_acc_val_date", "account_number, validation_date")],
            "cleanup": ["DELETE t1 FROM validation t1 INNER JOIN validation t2 ON t1.account_number = t2.account_number AND t1.validation_date = t2.validation_date WHERE t1.id < t2.id"]
        },
        {
            "name": "resolutions",
            "unique_keys": [("uniq_acc_res", "account_number")],
            "cleanup": ["DELETE t1 FROM resolutions t1 INNER JOIN resolutions t2 ON t1.account_number = t2.account_number WHERE t1.id < t2.id"]
        },
        {
            "name": "discounts",
            "unique_keys": [("uniq_acc_disc_date", "account_number, date_applied")],
            "cleanup": ["DELETE t1 FROM discounts t1 INNER JOIN discounts t2 ON t1.account_number = t2.account_number AND t1.date_applied = t2.date_applied WHERE t1.id < t2.id"]
        },
        {
            "name": "adjustments",
            "unique_keys": [("uniq_acc_adj_date", "account_number, date_applied")],
            "cleanup": ["DELETE t1 FROM adjustments t1 INNER JOIN adjustments t2 ON t1.account_number = t2.account_number AND t1.date_applied = t2.date_applied WHERE t1.id < t2.id"]
        },
        {
            "name": "accounts",
            "unique_keys": [("uniq_acc_num", "account_number")],
            "cleanup": ["DELETE t1 FROM accounts t1 INNER JOIN accounts t2 ON t1.account_number = t2.account_number WHERE t1.id < t2.id"]
        }
    ]

    for config in table_configs:
        t_name = config["name"]
        
        for key_name, columns in config["unique_keys"]:
            migration_id = f"unique_{t_name}_{key_name}"
            if is_applied(migration_id):
                continue
            
            # Double check the DB schema itself - maybe it was applied but not marked in meta
            try:
                with engine.connect() as conn:
                    res = conn.execute(text(f"SHOW INDEX FROM {t_name} WHERE Key_name = :k"), {"k": key_name})
                    if res.fetchone():
                        mark_applied(migration_id)
                        continue
            except Exception:
                pass

            logging.info(f"Applying deduplication to {t_name} ({columns})...")
            
            try:
                # 1. Create temporary non-unique indexes to speed up the DELETE if they don't exist
                # This makes the INNER JOIN much faster
                cols_list = [c.strip() for c in columns.split(',')]
                temp_idx_name = f"tmp_idx_{t_name}_{key_name}"
                
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f"CREATE INDEX {temp_idx_name} ON {t_name} ({columns})"))
                except Exception:
                    pass # Skip if already exists or fails

                # 2. Fast cleanup
                for cleanup_sql in config["cleanup"]:
                    with engine.begin() as conn:
                        result = conn.execute(text(cleanup_sql))
                        rows_deleted = result.rowcount
                        if rows_deleted > 0:
                            logging.info(f"  └ Deleted {rows_deleted:,} duplicate rows from {t_name}.")
                
                # 3. Drop temp index and apply Unique Key
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f"DROP INDEX {temp_idx_name} ON {t_name}"))
                except Exception:
                    pass
                    
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {t_name} ADD UNIQUE KEY {key_name} ({columns})"))
                
                mark_applied(migration_id)
                logging.info(f"  Successfully applied {key_name} to {t_name}.")
                
            except Exception as e:
                err = str(e)
                if "1061" in err or "1060" in err: # Already exists
                    mark_applied(migration_id)
                    logging.info(f"  Key {key_name} already exists on {t_name}.")
                elif "1062" in err:
                    logging.error(f"  CRITICAL: Could not apply {key_name} to {t_name} - duplicates still exist.")
                else:
                    logging.warning(f"  Failed step for {t_name}: {err[:100]}")

    # 4. Performance Indexes
    perf_indexes = [
        ("idx_bu_off_date", "accounts", "business_unit, account_officer"),
        ("idx_acc_num_coll", "collections", "account_number")
    ]

    for idx_name, table, cols in perf_indexes:
        migration_id = f"index_{idx_name}"
        if is_applied(migration_id):
            continue
            
        try:
            with engine.begin() as conn:
                conn.execute(text(f"CREATE INDEX {idx_name} ON {table} ({cols})"))
            mark_applied(migration_id)
            logging.info(f"Created index {idx_name} on {table}.")
        except Exception as e:
            if "1061" in str(e):
                mark_applied(migration_id)
            else:
                logging.warning(f"Failed to create index {idx_name}: {e}")

    # 5. other_payments Hash (Legacy Step - No longer needed as it is handled by table_configs above)
    mark_applied("other_payments_hash")

    # Step 10: Performance Indexes for Reporting
    try:
        with engine.begin() as conn:
            # Safer index creation check for older MySQL
            def create_idx_if_not_exists(table, idx_name, columns):
                try:
                    res = conn.execute(text(f"SHOW INDEX FROM {table} WHERE Key_name = :k"), {"k": idx_name}).fetchone()
                    if not res:
                        conn.execute(text(f"CREATE INDEX {idx_name} ON {table}({columns})"))
                except: pass

            create_idx_if_not_exists("collections", "idx_coll_date", "date_of_payment")
            create_idx_if_not_exists("other_payments", "idx_other_payment_date", "date_of_payment")
            create_idx_if_not_exists("customers", "idx_customers_name", "account_name")
            
            # Create/Update all_payments view (at the end, after all tables are sync'd)
            try:
                conn.execute(text("DROP TABLE IF EXISTS all_payments"))
            except: pass
            
            conn.execute(text("""
                CREATE OR REPLACE VIEW all_payments AS
                SELECT 
                    account_number, 
                    amount_paid, 
                    CAST(date_of_payment AS DATE) as date_of_payment, 
                    'collection' as payment_source 
                FROM collections
                UNION ALL
                SELECT 
                    account_number, 
                    amount_paid, 
                    CAST(date_of_payment AS DATE) as date_of_payment, 
                    COALESCE(payment_type, 'other') as payment_source 
                FROM other_payments;
            """))
            logging.info("Created/Updated all_payments view (Unified Deduplicated Source).")
    except Exception as e:
        logging.error(f"Migration Step 10 error (Indexes/Views): {e}")

if __name__ == "__main__":
    import sys
    import os
    # Add the project root to sys.path to allow importing from the root directory
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from db_utils import get_db_engine
    engine = get_db_engine()
    run_migrations(engine)
