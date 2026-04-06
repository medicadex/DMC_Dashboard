import os
import sys
import socket
import threading
from sqlalchemy import create_engine, text # type: ignore
from sqlalchemy.pool import NullPool
from urllib.parse import quote_plus
from dotenv import load_dotenv # type: ignore

# Global state for connectivity
_is_online = False
_connectivity_lock = threading.Lock()

def set_online_status(status):
    global _is_online
    with _connectivity_lock:
        _is_online = status

def is_online():
    with _connectivity_lock:
        return _is_online

def check_internet_connection(host="8.8.8.8", port=53, timeout=3):
    """
    Check if there is an active internet connection.
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error as ex:
        return False

def get_local_ip():
    """Returns the local IP address of the machine."""
    try:
        # Create a dummy socket to find the local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

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

def get_active_drive():
    """
    Returns the primary active drive (C: or D:) with sufficient space.
    """
    import shutil
    drives = ["D:", "C:"] # Prefer D: for data if it exists
    for drive in drives:
        drive_path = drive + "\\"
        if os.path.exists(drive_path):
            try:
                total, used, free = shutil.disk_usage(drive_path)
                # Ensure at least 5GB free
                if free > 5 * 1024 * 1024 * 1024:
                    return drive
            except Exception:
                continue
    return "C:"

def get_app_data_folder():
    """
    Returns the persistent storage directory for DB and Configs on the local laptop.
    Automatically detects and installs to the primary active drive.
    """
    active_drive = get_active_drive()
    dmc_dir = os.path.join(active_drive + "\\", "DMC")
    
    try:
        os.makedirs(dmc_dir, exist_ok=True)
        return dmc_dir
    except Exception:
        fallback = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), "DMC")
        os.makedirs(fallback, exist_ok=True)
        return fallback

def get_local_engine():
    """
    Creates and returns a SQLAlchemy engine for the local SQLite database.
    This is used for offline caching and synchronization.
    """
    db_path = os.path.join(get_app_data_folder(), "local_cache.db")
    # SQLite connection string
    conn_str = f"sqlite:///{db_path}"
    
    return create_engine(
        conn_str,
        poolclass=NullPool, # SQLite doesn't need connection pooling in the same way
        connect_args={'timeout': 30} # Increase timeout to 30s to prevent 'database is locked' errors
    )

def get_local_mysql_engine():
    """
    Creates and returns a SQLAlchemy engine for the LOCAL MySQL database.
    This is used for the user's personal backup and historical records.
    """
    db_user = "root"
    db_pass = "Loveth123."
    db_name = "debt_management"
    db_host = "localhost"
    
    encoded_pw = quote_plus(str(db_pass))
    conn_str = f"mysql+pymysql://{db_user}:{encoded_pw}@{db_host}:3306/{db_name}?charset=utf8mb4"
    
    return create_engine(
        conn_str,
        poolclass=NullPool,
        connect_args={'connect_timeout': 5}
    )

def get_db_engine(db_pass=None, include_db=True):
    """
    Creates and returns a SQLAlchemy engine for the database.
    Loads credentials from .env or prompts user if missing.
    If include_db is False, connects to the server without specifying a database.
    """
    load_env() # Ensure env is loaded before getting vars
    db_user = os.getenv("DB_USER", "admin")
    db_pass = db_pass or os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME", "dmc")
    
    # Check for placeholder in password
    if db_pass == "[AWS RDS password]":
        db_pass = None

    db_host = os.getenv("DB_HOST", "100.24.75.156")
    db_port = os.getenv("DB_PORT", "3306")

    from urllib.parse import quote_plus
    encoded_pw = quote_plus(str(db_pass)) if db_pass else ""
    # Standardizing on utf8mb4_0900_ai_ci for the entire pipeline
    if include_db:
        conn_str = f"mysql+pymysql://{db_user}:{encoded_pw}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    else:
        conn_str = f"mysql+pymysql://{db_user}:{encoded_pw}@{db_host}:{db_port}/?charset=utf8mb4"
    
    # Using NullPool to bypass connection pooling issues that were causing hangs
    from sqlalchemy.pool import NullPool
    return create_engine(
        conn_str,
        poolclass=NullPool,
        connect_args={'connect_timeout': 10}
    )

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
