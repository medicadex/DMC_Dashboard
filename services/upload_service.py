import pandas as pd  # type: ignore
import os
import json
import hashlib
from sqlalchemy import text  # type: ignore
from typing import Any
import logging
import uuid
import gc

# ────────────────────────────────────────────────────────────────────────────
# Web-native UploadService
# Architecture: All uploads go directly to RDS via a staging table.
# No temp_ outbox exists in the web app (that is a desktop-only concept).
# Deduplication is handled on RDS by the transaction_id unique key:
#   - REPLACE INTO  → master/reference tables (allows updates of existing rows)
#   - INSERT IGNORE → transactional tables   (silently skips true duplicates)
# ────────────────────────────────────────────────────────────────────────────

# Tables where a re-upload should UPDATE existing records (UPSERT / REPLACE)
UPSERT_TABLES = {'customers', 'performance_config', 'staff', 'discounts', 'adjustments', 'resolutions'}

# Tables where duplicate rows must be silently skipped (INSERT IGNORE)
# The transaction_id MD5 hash acts as the deduplication key.
IGNORE_TABLES = {'collections', 'other_payments', 'validation', 'disconnections'}


def _generate_transaction_id(row: pd.Series, table_name: str) -> str:
    """Deterministic MD5 hash used as the unique deduplication key per table."""
    def safe(val):
        return str(val).strip().lower() if pd.notnull(val) else ""

    if table_name == 'collections':
        base = f"{safe(row.get('account_number'))}_{safe(row.get('date_of_payment'))}_{safe(row.get('amount_paid'))}"
    elif table_name == 'other_payments':
        base = f"{safe(row.get('account_number'))}_{safe(row.get('date_of_payment'))}_{safe(row.get('amount_paid'))}"
    elif table_name == 'validation':
        base = f"{safe(row.get('account_number'))}_{safe(row.get('validation_date'))}"
    elif table_name == 'disconnections':
        base = f"{safe(row.get('account_number'))}_{safe(row.get('disconnection_date'))}"
    elif table_name == 'discounts':
        base = f"{safe(row.get('account_number'))}_{safe(row.get('date_applied'))}_{safe(row.get('discounted_amount'))}"
    elif table_name == 'adjustments':
        base = f"{safe(row.get('account_number'))}_{safe(row.get('date_applied'))}_{safe(row.get('adjustment_amount'))}"
    elif table_name == 'resolutions':
        base = f"{safe(row.get('account_number'))}_{safe(row.get('resolution_date'))}"
    else:
        # For tables without a natural key, fall back to a random UUID
        return str(uuid.uuid4())

    return hashlib.md5(base.encode('utf-8')).hexdigest()


class UploadService:
    def __init__(self, engine, staff_repo):
        self.engine = engine
        self.repo = staff_repo
        self.load_mappings()

    def load_mappings(self):
        try:
            from db_utils import get_project_folder
            map_path = os.path.join(get_project_folder(), "mappings.json")
            with open(map_path, "r") as f:
                self.FILES_CONFIG = json.load(f)
        except Exception:
            self.FILES_CONFIG = {}
            print("mappings.json not found. Uploads may fail.")

    def normalize_column_name(self, s):
        if not isinstance(s, str):
            return ""
        return s.strip().replace(" ", "").replace("-", "").replace("_", "").lower()

    # ──────────────────────────────────────────────────────────────────────────
    # Core upload pipeline (used by both standard uploader and admin uploader)
    # ──────────────────────────────────────────────────────────────────────────
    def _load_and_map_dataframe(self, table_name: str, filepath: str) -> pd.DataFrame:
        """Reads the Excel file and maps columns according to mappings.json."""
        df = pd.read_excel(filepath)

        config = self.FILES_CONFIG.get(table_name)
        mapping = config.get("mapping", {}) if isinstance(config, dict) else {}
        original_cols = {self.normalize_column_name(c): c for c in df.columns}

        if isinstance(mapping, dict) and mapping:
            df_mapped = pd.DataFrame()
            for excel_key, db_col in mapping.items():
                if excel_key in original_cols:
                    df_mapped[db_col] = df[original_cols[excel_key]]
        else:
            df_mapped = df.copy()

        if df_mapped.empty:
            raise ValueError(f"No matching columns found in '{os.path.basename(filepath)}'. "
                             f"Check that the file has the correct headers for '{table_name}'.")

        # Parse date columns
        if isinstance(config, dict):
            for date_col in config.get("date_cols", []):
                if date_col in df_mapped.columns:
                    df_mapped[date_col] = pd.to_datetime(
                        df_mapped[date_col], errors='coerce', format='mixed'
                    )

        # Clean numeric columns (strip currency symbols, commas, etc.)
        numeric_keywords = ['amount', 'balance', 'debt', 'percentage', 'discount']
        for col in df_mapped.columns:
            if any(kw in col.lower() for kw in numeric_keywords):
                df_mapped[col] = df_mapped[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                df_mapped[col] = pd.to_numeric(df_mapped[col], errors='coerce')

        return df_mapped

    def _push_to_rds(self, table_name: str, df_mapped: pd.DataFrame,
                     chunk_size: int = 5000, progress_callback=None) -> dict:
        """
        Core RDS push pipeline:
          1. Generate transaction_id deduplication keys (if not already present)
          2. Load data into a temporary staging table on RDS (chunked)
          3. Merge from staging → live table using REPLACE or INSERT IGNORE
          4. Drop the staging table (always, even on error)

        Returns a summary dict: {new, total, amount, duplicates}
        """
        # Mark all rows as SYNCED (web app is always online)
        df_mapped = df_mapped.copy()
        df_mapped['sync_status'] = 'SYNCED'

        # Generate deduplication keys
        if 'transaction_id' not in df_mapped.columns:
            df_mapped['transaction_id'] = df_mapped.apply(
                lambda row: _generate_transaction_id(row, table_name), axis=1
            )

        staging_table = f"staging_{table_name}_{uuid.uuid4().hex[:8]}"
        total_new = 0
        total_processed = 0
        total_amount = 0.0

        with self.engine.begin() as conn:
            # Fetch actual columns that exist in the RDS target table
            res_cols = conn.execute(text(f"DESCRIBE `{table_name}`"))
            target_cols = [row[0] for row in res_cols.fetchall()]

            cols_to_insert = [c for c in df_mapped.columns if c in target_cols]
            if not cols_to_insert:
                raise ValueError(
                    f"No columns in the uploaded file match the '{table_name}' table schema. "
                    f"Available DB columns: {target_cols}"
                )

            df_final = df_mapped[cols_to_insert]
            total_processed = len(df_final)

            try:
                # ── Step 1: Load into staging table (chunked) ──────────────────
                num_chunks = max(1, (total_processed // chunk_size) + 1)
                for i in range(num_chunks):
                    start_idx = i * chunk_size
                    end_idx = min((i + 1) * chunk_size, total_processed)
                    if start_idx >= end_idx:
                        break
                    chunk = df_final.iloc[start_idx:end_idx]
                    chunk.to_sql(
                        staging_table, conn,
                        if_exists="replace" if i == 0 else "append",
                        index=False
                    )
                    if progress_callback:
                        progress_callback(int(((i + 1) / num_chunks) * 90))

                # ── Step 2: Merge staging → live table on RDS ──────────────────
                # Deduplication strategy: Use ON DUPLICATE KEY UPDATE to avoid wiping skipped columns
                conflict_keys = {
                    'customers': 'account_number',
                    'staff': 'username',
                    'performance_config': 'bu_name',
                    'discounts': 'transaction_id',
                    'adjustments': 'transaction_id',
                    'resolutions': 'transaction_id'
                }
                
                conflict_key = conflict_keys.get(table_name)
                escaped_cols = ", ".join(f"`{c}`" for c in cols_to_insert)
                
                if conflict_key and conflict_key in cols_to_insert:
                    update_cols = [c for c in cols_to_insert if c != conflict_key]
                    if update_cols:
                        update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
                        merge_sql = text(
                            f"INSERT INTO `{table_name}` ({escaped_cols}) "
                            f"SELECT {escaped_cols} FROM `{staging_table}` "
                            f"ON DUPLICATE KEY UPDATE {update_clause}"
                        )
                    else:
                        merge_sql = text(
                            f"INSERT IGNORE INTO `{table_name}` ({escaped_cols}) "
                            f"SELECT {escaped_cols} FROM `{staging_table}`"
                        )
                else:
                    merge_sql = text(
                        f"INSERT IGNORE INTO `{table_name}` ({escaped_cols}) "
                        f"SELECT {escaped_cols} FROM `{staging_table}`"
                    )

                result = conn.execute(merge_sql)

                # rowcount semantics:
                #   INSERT IGNORE → rows actually inserted (duplicates = 0 affected)
                #   REPLACE       → rows inserted (2x per updated row due to delete+insert)
                if verb == "INSERT IGNORE":
                    total_new = result.rowcount
                else:
                    # REPLACE reports 2 for each updated row, 1 for new rows
                    # Divide by 2 gives a rough "updates" count; report total processed instead
                    total_new = min(result.rowcount, total_processed)

                if 'amount_paid' in df_final.columns:
                    total_amount = float(df_final['amount_paid'].sum())

                if progress_callback:
                    progress_callback(100)

            finally:
                # ── Step 3: Always clean up the staging table ──────────────────
                try:
                    conn.execute(text(f"DROP TABLE IF EXISTS `{staging_table}`"))
                except Exception as cleanup_err:
                    logging.warning(f"Failed to drop staging table '{staging_table}': {cleanup_err}")

        duplicates = max(0, total_processed - total_new) if verb == "INSERT IGNORE" else 0
        return {
            "new": total_new,
            "total": total_processed,
            "amount": total_amount,
            "duplicates": duplicates,
            "verb": verb
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Public Methods
    # ──────────────────────────────────────────────────────────────────────────

    def process_table(self, table_name: str, filepath: str, username: str,
                      progress_callback=None) -> dict:
        """
        Standard upload entry point (used by /report-uploader and /admin/upload-tables).
        Maps columns from the Excel file via mappings.json, then pushes directly to RDS
        with deduplication handled by transaction_id on the staging table.
        """
        self.load_mappings()  # Hot-reload so mappings.json edits take effect instantly

        if not os.path.isfile(filepath):
            return {"new": 0, "total": 0, "amount": 0.0, "duplicates": 0}

        self.repo.log_activity(
            username, "UPLOAD_START",
            f"Table: {table_name}, File: {os.path.basename(filepath)}"
        )

        try:
            df_mapped = self._load_and_map_dataframe(table_name, filepath)
            
            # Detect officer changes programmatically before writing to DB
            officer_changes = {}
            if table_name.lower() in ['customers', 'accounts']:
                try:
                    from services.account_service import AccountService
                    account_service = AccountService(self.engine, self.repo, None)
                    officer_changes = account_service.detect_officer_changes_in_df(df_mapped)
                except Exception as e:
                    logging.error(f"Error detecting officer changes programmatically in web: {e}")
                    
            result = self._push_to_rds(table_name, df_mapped, progress_callback=progress_callback)
            result['officer_changes'] = officer_changes

            self.repo.log_activity(
                username, "UPLOAD_SUCCESS",
                f"Table: {table_name} | Rows: {result['total']} | "
                f"New/Updated: {result['new']} | Duplicates skipped: {result['duplicates']}",
                event_type='MAJOR'
            )

            # Free memory
            del df_mapped
            gc.collect()

            return result

        except Exception as e:
            self.repo.log_activity(username, "UPLOAD_ERROR", str(e), event_type='MAJOR')
            logging.error(f"Upload failed for '{table_name}': {e}")
            raise

    def process_excel_upload(self, table_name: str, filepath: str, username: str,
                             manual_mapping: dict = None, progress_callback=None) -> dict:
        """
        Bulk upload entry point with optional manual column mapping.
        Used when the Excel headers don't match mappings.json and a custom
        column mapping dict is provided by the caller.
        """
        if not os.path.isfile(filepath):
            return {"new": 0, "total": 0, "amount": 0.0, "duplicates": 0}

        self.repo.log_activity(
            username, "BULK_UPLOAD_START",
            f"Table: {table_name}, File: {os.path.basename(filepath)}"
        )

        try:
            df = pd.read_excel(filepath)

            if manual_mapping:
                df_mapped = df.rename(columns=manual_mapping)
            else:
                df_mapped = df.copy()

            # Detect officer changes programmatically before writing to DB
            officer_changes = {}
            if table_name.lower() in ['customers', 'accounts']:
                try:
                    from services.account_service import AccountService
                    account_service = AccountService(self.engine, self.repo, None)
                    officer_changes = account_service.detect_officer_changes_in_df(df_mapped)
                except Exception as e:
                    logging.error(f"Error detecting officer changes programmatically in web: {e}")

            result = self._push_to_rds(table_name, df_mapped, progress_callback=progress_callback)
            result['officer_changes'] = officer_changes

            self.repo.log_activity(
                username, "BULK_UPLOAD_SUCCESS",
                f"Table: {table_name} | Rows: {result['total']} | "
                f"New/Updated: {result['new']} | Duplicates skipped: {result['duplicates']}",
                event_type='MAJOR'
            )

            del df, df_mapped
            gc.collect()

            return result

        except Exception as e:
            self.repo.log_activity(username, "BULK_UPLOAD_ERROR", str(e), event_type='MAJOR')
            logging.error(f"Bulk upload failed for '{table_name}': {e}")
            raise
