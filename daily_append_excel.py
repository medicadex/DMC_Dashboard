import pandas as pd
from datetime import datetime
import os
import sys
import logging
import warnings
import glob
from cleanup_utils import setup_temp_cleanup
import tkinter as tk
from tkinter import ttk, messagebox

from sqlalchemy import text
from db_utils import get_project_folder

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
FOLDER = get_project_folder()

DUMP_DIR = os.path.join(FOLDER, "dump")
os.makedirs(DUMP_DIR, exist_ok=True)

FILES_CONFIG = {
    "collections": {
        "filename": "Report_*.xlsx",
        "sheet": 0,
        "date_cols": ["date_of_payment"],
        "mapping": {
            "accountnumber": "account_number", "accountname": "account_name", "address": "account_address",
            "dateofpayment": "date_of_payment", "debtbalancelastpayment": "debt_balance_last_payment",
            "amountpaid": "amount_paid", "currentbalance": "current_balance",
            "transactionid": "transaction_id", "receiptnumber": "receipt_number",
            "businessunit": "business_unit"
        }
    },
    "validation": {
        "filename": "CustomerValidation_*.xlsx",
        "sheet": 0,
        "date_cols": ["validation_date"],
        "mapping": {
            "date": "validation_date", "accountnumber": "account_number", "accountname": "account_name",
            "customeraddress": "account_address", "phonenumber": "phone_number", "physicalstatus": "physical_status",
            "gpscoordinate": "gps_coordinate", "reasonfornonpayment": "reason_for_non_payment",
            "dmo": "dmo", "dms": "dms", "actionrequired": "action_required"
        }
    },
    # Add other types as needed
}

def normalize_column_name(s):
    if not isinstance(s, str): return ""
    return s.strip().replace(" ", "").replace("-", "").replace("_", "").lower()

class DataUploader:
    def __init__(self, parent_frame, log_callback, engine=None):
        self.parent = parent_frame
        self.log = log_callback
        self.engine = engine
        
    def run_upload(self, selected_tables=None):
        warnings.filterwarnings("ignore", message="Workbook contains no default style, apply openpyxl's default")
        logging.info("Historical re-upload started")
        total_rows = 0
        
        tables_to_process = selected_tables if selected_tables else FILES_CONFIG.keys()
        
        # Determine search folder (look in parent if running from dist/)
        search_folder = FOLDER
        if getattr(sys, 'frozen', False) and not any(glob.glob(os.path.join(search_folder, "*.xlsx"))):
            parent = os.path.dirname(search_folder)
            if any(glob.glob(os.path.join(parent, "*.xlsx"))):
                search_folder = parent

        for table_name in tables_to_process:
            if table_name not in FILES_CONFIG: continue
            config = FILES_CONFIG[table_name]
            pattern = config["filename"]
            files = glob.glob(os.path.join(search_folder, pattern))
            files = [f for f in files if "duplicates" not in os.path.basename(f).lower()]
            
            self.log(f"Found {len(files)} files for {table_name}")
            for filepath in files:
                cfg = config.copy()
                cfg["filename"] = filepath # Use full path
                try:
                    count = self.process_table(table_name, cfg)
                    total_rows += count
                except Exception as e:
                    self.log(f"Error in {table_name} for {filepath}: {str(e)}")
                    
        self.log(f"Finished — total rows processed: {total_rows:,}")
        messagebox.showinfo("Upload Complete", f"Processed {total_rows:,} rows across all files.")

    def process_table(self, table_name, cfg):
        filepath = cfg["filename"]
        if not os.path.isfile(filepath): return 0

        self.log(f"Processing {table_name} → {filepath}")
        try:
            df = pd.read_excel(filepath)
            
            # Clean column names
            original_cols = {normalize_column_name(c): c for c in df.columns}
            mapping = cfg["mapping"]
            
            # Build mapped dataframe
            df_mapped = pd.DataFrame()
            for excel_key, db_col in mapping.items():
                if excel_key in original_cols:
                    df_mapped[db_col] = df[original_cols[excel_key]]
            
            if df_mapped.empty:
                self.log(f"  ⚠ No matching columns in {filepath}")
                return 0
            
            # Handle dates
            for date_col in cfg.get("date_cols", []):
                if date_col in df_mapped.columns:
                    df_mapped[date_col] = pd.to_datetime(df_mapped[date_col], errors='coerce')

            with self.engine.begin() as conn:
                # Filter target columns
                res_target_cols = conn.execute(text(f"DESCRIBE {table_name}"))
                target_cols = [row[0] for row in res_target_cols.fetchall()]
                cols_to_insert = [c for c in df_mapped.columns if c in target_cols]
                
                if not cols_to_insert:
                    self.log(f"  ⚠ No database columns match for {table_name}")
                    return 0

                df_final = df_mapped[cols_to_insert]
                staging_table = f"staging_{table_name}"
                df_final.to_sql(staging_table, conn, if_exists="replace", index=False)
                
                cols = list(df_final.columns)
                unique_col = None
                if 'transaction_id' in cols: unique_col = 'transaction_id'
                elif 'receipt_number' in cols: unique_col = 'receipt_number'
                
                if unique_col:
                    # Duplicate check
                    existing_count_sql = text(f"SELECT COUNT(*) FROM {table_name} WHERE {unique_col} IN (SELECT {unique_col} FROM {staging_table})")
                    duplicates_count = conn.execute(existing_count_sql).scalar()
                    
                    sql = text(f"""
                        INSERT INTO {table_name} ({', '.join(cols)}) 
                        SELECT {', '.join(cols)} FROM {staging_table} s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {table_name} t WHERE t.{unique_col} = s.{unique_col}
                        )
                    """)
                else:
                    duplicates_count = 0
                    sql = text(f"INSERT IGNORE INTO {table_name} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {staging_table}")
                    
                result = conn.execute(sql)
                new_records = result.rowcount
                
                total_amount = float(df_final['amount_paid'].sum()) if 'amount_paid' in df_final.columns else 0.0
                
                self.log(f"  ✅ Processed {len(df_final)} records.")
                self.log(f"    └ New: {new_records}, Duplicates: {duplicates_count}")
                if total_amount > 0:
                    self.log(f"    └ Total Amount: ₦{total_amount:,.2f}")
                
                return new_records
                
        except Exception as e:
            self.log(f"  ❌ Error: {str(e)}")
            return 0

if __name__ == "__main__":
    pass
