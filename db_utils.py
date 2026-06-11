import os
import sys
from sqlalchemy import create_engine, text, exc # type: ignore
from sqlalchemy.pool import QueuePool
from urllib.parse import quote_plus
from dotenv import load_dotenv # type: ignore

# Load environment variables
def load_env():
    """Loads .env from the project folder."""
    env_path = os.path.join(get_project_folder(), ".env")
    if os.path.exists(env_path):
        print(f"[DEBUG] Loading .env from {env_path}")
        load_dotenv(env_path)
    else:
        print("[DEBUG] .env file not found in project folder.")
        load_dotenv() # Fallback to default search

def get_db_engine(db_pass=None, include_db=True):
    """
    Creates and returns a SQLAlchemy engine for the Supabase PostgreSQL database.
    Loads credentials from .env or prompts user if missing.
    If include_db is False, connects to the server without specifying a database.
    """
    load_env() # Ensure env is loaded before getting vars
    
    # Supabase PostgreSQL configuration
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = db_pass or os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME", "postgres")
    
    if db_pass == "[AWS RDS password]":
        db_pass = None

    db_host = os.getenv("DB_HOST", "aws-0-eu-west-1.pooler.supabase.com")
    db_port = os.getenv("DB_PORT", "5432")
    db_ssl_ca = os.getenv("DB_SSL_CA")

    encoded_pw = quote_plus(str(db_pass)) if db_pass else ""
    
    # Supabase PostgreSQL connection string
    if include_db:
        conn_str = f"postgresql+psycopg2://{db_user}:{encoded_pw}@{db_host}:{db_port}/{db_name}"
    else:
        conn_str = f"postgresql+psycopg2://{db_user}:{encoded_pw}@{db_host}:{db_port}/postgres"
    
    # Supabase Connection Arguments
    connect_args = {
        'sslmode': 'require',
        'connect_timeout': 15
    }
    
    if db_ssl_ca:
        ca_path = db_ssl_ca
        if not os.path.isabs(ca_path):
            ca_path = os.path.join(get_project_folder(), ca_path)
            
        if os.path.exists(ca_path):
            connect_args['sslrootcert'] = ca_path
            connect_args['sslmode'] = 'verify-full'
        else:
            print(f"[WARNING] SSL CA file not found at: {ca_path}")

    try:
        engine = create_engine(
            conn_str,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800, # Recycle connections after 30 mins to avoid stale connections
            pool_pre_ping=True, # Verify connection is alive before using it
            connect_args=connect_args
        )
        return engine
    except exc.SQLAlchemyError as e:
        print(f"[ERROR] Database connection failed: {e}")
        raise

def get_project_folder():
    """
    Returns the absolute path to the project root directory.
    When frozen as an EXE, this returns the directory of the EXE.
    """
    if getattr(sys, 'frozen', False):
        # If the app is compiled as a single EXE, PyInstaller extracts data files to sys._MEIPASS
        if hasattr(sys, '_MEIPASS'):
            return sys._MEIPASS
        return os.path.dirname(sys.executable)
    
    # Otherwise, it's the directory of this script
    return os.path.dirname(os.path.abspath(__file__))

def execute_sql_script(script_path):
    engine = get_db_engine()
    with open(script_path, "r") as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
