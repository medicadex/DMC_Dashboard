import pandas as pd # type: ignore
import os
import json
from sqlalchemy import text # type: ignore
from typing import Any
import logging
import uuid
from utils.security import SecurityManager # type: ignore
import gc
from db_utils import is_online, get_local_engine # type: ignore

class UploadService:
    def __init__(self, engine, staff_repo):
        self.engine = engine
        self.local_engine = get_local_engine()
        self.repo = staff_repo
        self.load_mappings()

    def load_mappings(self):
        try:
            from db_utils import get_project_folder
            import os
            map_path = os.path.join(get_project_folder(), "mappings.json")
            with open(map_path, "r") as f:
                self.FILES_CONFIG = json.load(f)
        except Exception as e:
            self.FILES_CONFIG = {}
            print("mappings.json not found. Uploads may fail.")

    def normalize_column_name(self, s):
        if not isinstance(s, str): return ""
        return s.strip().replace(" ", "").replace("-", "").replace("_", "").lower()

    def process_table(self, table_name, filepath, username, progress_callback=None):
        """Reconstructed legacy logic from daily_append_excel.py with chunked processing."""
        self.load_mappings() # Hot-reload config so file edits work instantly
        if not os.path.isfile(filepath): return {"new": 0, "total": 0}

        try:
            # Audit Every Upload (Step 7a)
            self.repo.log_activity(username, "UPLOAD_START", f"Table: {table_name}, File: {os.path.basename(filepath)}")

            # 1. Read metadata and total rows
            df_info = pd.read_excel(filepath, nrows=1)
            # We use a smaller chunk for reading if the file is massive, 
            # but for 50,000-byte chunk compliance (Step 7b), 
            # we'll process the dataframe in chunks.
            
            chunk_size = 5000 # Approximately 50,000 bytes worth of data depending on row width
            total_new = 0
            total_processed = 0
            total_amount = 0.0

            # Get total rows for progress tracking
            # Note: read_excel doesn't support chunksize like read_csv directly without engine='openpyxl'
            # For .xlsx, we'll read the whole file but process the DB transmission in chunks
            df = pd.read_excel(filepath)
            total_rows = len(df)
            
            config = self.FILES_CONFIG.get(table_name)
            
            # ... existing mapping logic ...
            original_cols = {self.normalize_column_name(c): c for c in df.columns}
            mapping = config.get("mapping", {}) if config else {}
            
            df_mapped = pd.DataFrame()
            if isinstance(mapping, dict) and mapping:
                for excel_key, db_col in mapping.items():
                    if excel_key in original_cols:
                        df_mapped[db_col] = df[original_cols[excel_key]]
            else:
                # Fallback for generic
                df_mapped = df.copy()

            if df_mapped.empty:
                raise ValueError(f"No matching columns in {os.path.basename(filepath)}")
            
            # Handle dates
            if isinstance(config, dict):
                conf_any: Any = config
                for date_col in conf_any.get("date_cols", []):
                    if date_col in df_mapped.columns:
                        df_mapped[date_col] = pd.to_datetime(df_mapped[date_col], errors='coerce', format='mixed')

            # Clean strictly numeric columns corrupted by trailing characters (commas, %, Naira symbols)
            numeric_keywords = ['amount', 'balance', 'debt', 'percentage', 'discount']
            for col in df_mapped.columns:
                if any(kw in col.lower() for kw in numeric_keywords):
                    # Filter out commas, symbols and convert to float so DB doesn't truncate at the first comma
                    df_mapped[col] = df_mapped[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                    df_mapped[col] = pd.to_numeric(df_mapped[col], errors='coerce')

            # Offline-First Strategy: Always write to Local SQLite first
            df_mapped['sync_status'] = 'PENDING'
            if 'transaction_id' not in df_mapped.columns:
                import hashlib
                def generate_tx_id(row):
                    def safe_str(val):
                        return str(val).strip().lower() if pd.notnull(val) else ""
                    base_string = ""
                    if table_name == 'validation':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('validation_date'))}"
                    elif table_name == 'disconnections':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('disconnection_date'))}"
                    elif table_name == 'discounts':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('date_applied'))}_{safe_str(row.get('discounted_amount'))}"
                    elif table_name == 'adjustments':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('date_applied'))}_{safe_str(row.get('adjustment_amount'))}"
                    elif table_name == 'collections':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('date_of_payment'))}_{safe_str(row.get('amount_paid'))}"
                    else:
                        return str(uuid.uuid4())
                    return hashlib.md5(base_string.encode('utf-8')).hexdigest()
                df_mapped['transaction_id'] = df_mapped.apply(generate_tx_id, axis=1)
            
            with self.local_engine.begin() as conn:
                res_target_cols = conn.execute(text(f"PRAGMA table_info({table_name})"))
                target_cols = [row[1] for row in res_target_cols.fetchall()]
                
                cols_to_insert = [c for c in df_mapped.columns if c in target_cols]
                if not cols_to_insert:
                    raise ValueError(f"No database columns match for {table_name}")

                df_final = df_mapped[cols_to_insert]
                staging_table = f"staging_{table_name}_{uuid.uuid4().hex[:8]}"
                
                # ... (rest of the logic remains similar but using local_engine)
                num_chunks = (len(df_final) // chunk_size) + 1
                for i in range(num_chunks):
                    start_idx = i * chunk_size
                    end_idx = min((i + 1) * chunk_size, len(df_final))
                    if start_idx >= end_idx: break
                    chunk = df_final.iloc[start_idx:end_idx]
                    chunk.to_sql(staging_table, conn, if_exists="replace" if i == 0 else "append", index=False)
                    if progress_callback: progress_callback(int(((i + 1) / num_chunks) * 100))

                cols = list(df_final.columns)
                target_outbox = f"temp_{table_name}" if table_name in ['collections', 'other_payments', 'validation', 'disconnections', 'resolutions', 'discounts', 'adjustments'] else table_name
                sql = text(f"INSERT OR IGNORE INTO {target_outbox} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {staging_table}")
                result = conn.execute(sql)
                total_new = result.rowcount
                total_processed = len(df_final)
                if 'amount_paid' in df_final.columns:
                    total_amount = float(df_final['amount_paid'].sum())
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

            # Audit log (Moved outside connection block to prevent SQLite locks)
            self.repo.log_activity(username, "UPLOAD_SUCCESS", f"Table: {table_name}, New: {total_new}", event_type='MAJOR')
            
            # Free memory
            del df, df_mapped, df_final
            gc.collect()

            return {"new": total_new, "total": total_processed, "amount": total_amount}

        except Exception as e:
            self.repo.log_activity(username, "UPLOAD_ERROR", str(e), event_type='MAJOR')
            logging.error(f"Upload failed for {table_name}: {e}")
            raise e

    def process_excel_upload(self, table_name, filepath, username, manual_mapping=None, progress_callback=None):
        """Processes bulk Excel uploads with chunking and progress tracking."""
        if not os.path.isfile(filepath): return {"new": 0, "total": 0}

        try:
            # Audit Every Upload (Step 7a)
            self.repo.log_activity(username, "BULK_UPLOAD_START", f"Table: {table_name}, File: {os.path.basename(filepath)}")

            df = pd.read_excel(filepath)
            total_rows = len(df)
            chunk_size = 5000 

            # Map columns
            if manual_mapping:
                df_mapped = df.rename(columns=manual_mapping)
            else:
                df_mapped = df.copy()

            # Offline-First Strategy: Always write to Local SQLite first
            df_mapped['sync_status'] = 'PENDING'
            if 'transaction_id' not in df_mapped.columns:
                import hashlib
                def generate_tx_id_bulk(row):
                    def safe_str(val):
                        return str(val).strip().lower() if pd.notnull(val) else ""
                    base_string = ""
                    if table_name == 'validation':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('validation_date'))}"
                    elif table_name == 'disconnections':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('disconnection_date'))}"
                    elif table_name == 'discounts':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('date_applied'))}_{safe_str(row.get('discounted_amount'))}"
                    elif table_name == 'adjustments':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('date_applied'))}_{safe_str(row.get('adjustment_amount'))}"
                    elif table_name == 'collections':
                        base_string = f"{safe_str(row.get('account_number'))}_{safe_str(row.get('date_of_payment'))}_{safe_str(row.get('amount_paid'))}"
                    else:
                        return str(uuid.uuid4())
                    return hashlib.md5(base_string.encode('utf-8')).hexdigest()
                df_mapped['transaction_id'] = df_mapped.apply(generate_tx_id_bulk, axis=1)

            with self.local_engine.begin() as conn:
                res_target_cols = conn.execute(text(f"PRAGMA table_info({table_name})"))
                target_cols = [row[1] for row in res_target_cols.fetchall()]

                cols_to_insert = [c for c in df_mapped.columns if c in target_cols]
                if not cols_to_insert:
                    raise ValueError(f"No database columns match for {table_name}")

                df_final = df_mapped[cols_to_insert]
                staging_table = f"staging_bulk_{table_name}_{uuid.uuid4().hex[:8]}"
                
                # Chunked Transmission
                num_chunks = (len(df_final) // chunk_size) + 1
                for i in range(num_chunks):
                    start_idx = i * chunk_size
                    end_idx = min((i + 1) * chunk_size, len(df_final))
                    if start_idx >= end_idx: break
                    chunk = df_final.iloc[start_idx:end_idx]
                    chunk.to_sql(staging_table, conn, if_exists="replace" if i == 0 else "append", index=False)
                    if progress_callback: progress_callback(int(((i + 1) / num_chunks) * 100))

                cols = list(df_final.columns)
                target_outbox = f"temp_{table_name}" if table_name in ['collections', 'other_payments', 'validation', 'disconnections', 'resolutions', 'discounts', 'adjustments'] else table_name
                sql = text(f"INSERT OR IGNORE INTO {target_outbox} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {staging_table}")
                result = conn.execute(sql)
                new_records = result.rowcount
                total_len = len(df_final)
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

            # Audit log (Moved outside connection block to prevent SQLite locks)
            self.repo.log_activity(username, "BULK_UPLOAD_SUCCESS", f"Table: {table_name}, New: {new_records}", event_type='MAJOR')
            
            # Free memory
            del df, df_mapped, df_final
            gc.collect()

            return {"new": new_records, "total": total_len}

        except Exception as e:
            self.repo.log_activity(username, "BULK_UPLOAD_ERROR", str(e), event_type='MAJOR')
            raise e
