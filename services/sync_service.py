import threading
import logging
import time
import uuid
from sqlalchemy import text
import pandas as pd

class SyncService:
    def __init__(self, local_engine, cloud_engine):
        self.local_engine = local_engine
        self.cloud_engine = cloud_engine
        self._sync_lock = threading.RLock()
        self.tables_to_push = ['staff', 'collections', 'other_payments', 'validation', 'user_activity_log', 'disconnections', 'resolutions', 'discounts', 'adjustments']
        self._stop_event = threading.Event()
        self._start_time = None
        self._backoff_delay = 15 # Initial delay for exponential backoff
        self._max_backoff = 300  # Max delay 5 minutes
        self._last_rds_backup_check = 0

    def sync_rds_to_local_mysql(self, local_mysql_engine, progress_callback=None):
        """
        Synchronizes all data from Cloud RDS back to Local MySQL (Workbench).
        Incremental: Only fetches records with id > max(local_id).
        This is an administrative backup function.
        """
        if not self._sync_lock.acquire(blocking=False):
            return

        try:
            tables = [
                'staff', 'customers', 'collections', 'other_payments', 
                'validation', 'resolutions', 'discounts', 'adjustments',
                'disconnections', 'performance_config', 'user_activity_log'
            ]
            
            total = len(tables)
            for i, table in enumerate(tables):
                if progress_callback:
                    progress_callback(table, int((i/total)*100), f"Incremental backup: {table}...", 0)
                
                # 1. Get max ID from Local MySQL
                try:
                    with local_mysql_engine.connect() as local_conn:
                        # Check if 'id' column exists
                        res_cols = local_conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE 'id'"))
                        has_id = res_cols.fetchone() is not None
                        
                        if has_id:
                            res = local_conn.execute(text(f"SELECT MAX(id) FROM {table}"))
                            max_id = res.scalar() or 0
                        else:
                            max_id = -1 # Signal to do a full sync if no id column
                except Exception as e:
                    # If table doesn't exist locally, start from 0
                    max_id = 0
                    has_id = True # Assume we'll create it with id
                
                # 2. Fetch records from RDS
                if has_id and max_id >= 0:
                    query = f"SELECT * FROM {table} WHERE id > {max_id}"
                else:
                    query = f"SELECT * FROM {table}" # Full sync if no id
                with self.cloud_engine.connect() as cloud_conn:
                    # Use chunking for large tables
                    chunk_list = []
                    for chunk in pd.read_sql_query(query, cloud_conn, chunksize=5000):
                        chunk_list.append(chunk)
                    
                    if not chunk_list:
                        continue
                    
                    df = pd.concat(chunk_list)
                
                if not df.empty:
                    # 3. Append to Local MySQL
                    with local_mysql_engine.begin() as local_conn:
                        df.to_sql(table, local_conn, if_exists='append', index=False, chunksize=1000)
            
            if progress_callback:
                progress_callback("Done", 100, "Incremental Backup to Local MySQL Complete.", 0)
                
        except Exception as e:
            logging.error(f"Failed RDS to Local MySQL Sync: {e}")
        finally:
            self._sync_lock.release()

    def get_pending_count(self):
        """Returns total number of records across all tables waiting to be pushed."""
        total = 0
        try:
            with self.local_engine.connect() as conn:
                for table in self.tables_to_push:
                    if table in ['collections', 'other_payments', 'validation', 'disconnections', 'resolutions', 'discounts', 'adjustments']:
                        try:
                            res = conn.execute(text(f"SELECT COUNT(*) FROM temp_{table}"))
                            total += res.scalar() or 0
                        except Exception:
                            pass # Table might not exist yet on very first run
                    else:
                        res = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE sync_status = 'PENDING'"))
                        total += res.scalar() or 0
        except Exception as e:
            logging.error(f"Error getting pending count: {e}")
        return total

    def cancel_sync(self):
        """Signals the sync process to stop."""
        self._stop_event.set()
        logging.info("Sync cancellation requested.")

    def _get_eta(self, current_percentage):
        """Calculates estimated time remaining in seconds."""
        if not self._start_time or current_percentage <= 0:
            return 0
        elapsed = time.time() - self._start_time
        total_est = elapsed / (current_percentage / 100.0)
        return int(total_est - elapsed)

    def is_syncing(self):
        """Returns True if a sync operation is currently in progress."""
        if self._sync_lock.acquire(blocking=False):
            self._sync_lock.release()
            return False
        return True

    def push_pending_to_cloud(self, progress_callback=None):
        """
        Pushes all 'PENDING' records from local SQLite to Cloud RDS.
        Includes summary metrics and notifications.
        """
        if not self._sync_lock.acquire(blocking=False):
            return

        try:
            self._stop_event.clear()
            total_tables = len(self.tables_to_push)
            global_summary = [] # List of dicts per table

            for i, table in enumerate(self.tables_to_push):
                if self._stop_event.is_set(): break
                
                # Update UI
                perc = int((i / total_tables) * 100)
                if progress_callback:
                    progress_callback("Sync", perc, f"Pushing {table}...", 0)

                # 1. Fetch PENDING records
                with self.local_engine.connect() as local_conn:
                    if table in ['collections', 'other_payments', 'validation', 'disconnections', 'resolutions', 'discounts', 'adjustments']:
                        df = pd.read_sql_query(f"SELECT * FROM temp_{table}", local_conn)
                    else:
                        df = pd.read_sql_query(f"SELECT * FROM {table} WHERE sync_status = 'PENDING'", local_conn)
                
                if df.empty:
                    continue

                # Decrypt sensitive fields if pushing 'staff' table
                if table == 'staff':
                    from utils.security import SecurityManager
                    if 'email' in df.columns:
                        df['email'] = df['email'].apply(lambda x: SecurityManager.decrypt_data(x) if x else x)
                    if 'phone_number' in df.columns:
                        df['phone_number'] = df['phone_number'].apply(lambda x: SecurityManager.decrypt_data(x) if x else x)
                    
                    # Special Case: full_name might be a generated column in RDS
                    if 'full_name' in df.columns:
                        df = df.drop(columns=['full_name'])

                # 2. Push to RDS
                success, table_metrics = self._push_with_retry(table, df)
                
                if success:
                    # 3. Update local cache to 'SYNCED' or Clear Temp Stack
                    with self.local_engine.begin() as local_conn:
                        if table in ['collections', 'other_payments', 'validation', 'disconnections', 'resolutions', 'discounts', 'adjustments']:
                            pushed_ids = df['transaction_id'].tolist() if 'transaction_id' in df.columns else []
                            if pushed_ids:
                                for chunk_idx in range(0, len(pushed_ids), 900):
                                    chunk = pushed_ids[chunk_idx:chunk_idx+900]
                                    placeholders = ','.join(['?'] * len(chunk))
                                    # We use native SQLite parameter binding to prevent drop failures on large packets
                                    # Wait, pd.read_sql uses SQLAlchemy Engine directly.
                                    # It's cleaner to bind correctly via raw string for IN, or use execute multiple.
                                    # An easier cross-compatible way for SQLAlchemy is text + dict of params, but since we trust generating UUIDs:
                                    local_conn.execute(text(f"DELETE FROM temp_{table} WHERE transaction_id IN ({','.join(repr(x) for x in chunk)})"))
                            else:
                                local_conn.execute(text(f"DELETE FROM temp_{table}"))
                        else:
                            local_conn.execute(text(f"UPDATE {table} SET sync_status = 'SYNCED' WHERE sync_status = 'PENDING'"))
                    
                    # Add to global summary
                    table_metrics['table'] = table
                    global_summary.append(table_metrics)

            # Finalize
            if progress_callback:
                if global_summary:
                    # Trigger an incremental pull to drop accepted outbox items back into our Main Read DB
                    outbox_tables_pushed = [m['table'] for m in global_summary if m['table'] in ['collections', 'other_payments', 'validation', 'disconnections', 'resolutions', 'discounts', 'adjustments']]
                    if outbox_tables_pushed:
                        progress_callback("Fetch", 95, f"Syncing {len(outbox_tables_pushed)} updated tables centrally...", 0)
                        self.pull_from_cloud(progress_callback, table_subset=outbox_tables_pushed)

                    # Send special "sync_summary" message to UI
                    progress_callback("Summary", 100, global_summary, 0)
                else:
                    progress_callback("Done", 100, "Sync Complete (No pending data).", 0)
                    
        except Exception as e:
            logging.error(f"Error during push_pending_to_cloud: {e}")
        finally:
            self._sync_lock.release()

    def _push_with_retry(self, table, df):
        """Internal helper to push data to RDS with conflict resolution and summary metrics."""
        summary = {"total": len(df), "success": 0, "duplicates": 0, "error": None}
        try:
            # Get actual columns in the Cloud RDS table to avoid "Unknown column" errors
            with self.cloud_engine.connect() as cloud_conn:
                res = cloud_conn.execute(text(f"SHOW COLUMNS FROM {table}"))
                cloud_cols = [row[0].lower() for row in res.fetchall()]
            
            # Exclude local-only columns and any columns that don't exist in the Cloud
            cols_to_drop = ['id'] if 'id' in df.columns else []
            for col in df.columns:
                if col.lower() not in cloud_cols:
                    cols_to_drop.append(col)
            
            df_to_push = df.drop(columns=list(set(cols_to_drop)))
            
            if 'sync_status' in df_to_push.columns:
                df_to_push['sync_status'] = 'SYNCED'

            with self.cloud_engine.begin() as cloud_conn:
                staging_table = f"staging_sync_{table}_{uuid.uuid4().hex[:8]}"
                df_to_push.to_sql(staging_table, con=cloud_conn, if_exists="replace", index=False)
                
                cols = list(df_to_push.columns)
                col_str = ", ".join(cols)
                
                # Determine Conflict Resolution Strategy
                verb = "REPLACE" if table in ['customers', 'performance_config', 'staff'] else "INSERT IGNORE"
                
                # Execute transfer from staging to main table
                res = cloud_conn.execute(text(f"{verb} INTO {table} ({col_str}) SELECT {col_str} FROM {staging_table}"))
                
                # Get number of rows actually inserted/updated
                summary["success"] = res.rowcount
                summary["duplicates"] = summary["total"] - summary["success"] if verb == "INSERT IGNORE" else 0
                
                cloud_conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
            
            return True, summary
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Failed to push to RDS for {table}: {error_msg}")
            summary["error"] = error_msg
            return False, summary

    def pull_from_cloud(self, progress_callback=None, table_subset=None):
        """
        Pulls down 'Last Known State' from Cloud RDS to local SQLite.
        Includes intelligent incremental fetching to save data.
        :param table_subset: List of table names to pull. If None, pulls all standard tables.
        """
        if not self._sync_lock.acquire(blocking=False):
            return

        self._stop_event.clear()
        self._start_time = time.time()

        try:
            # Full configuration for all pullable tables
            all_configs = [
                {'name': 'staff', 'query': "SELECT * FROM staff WHERE status = 'Active'"},
                {'name': 'performance_config', 'query': "SELECT * FROM performance_config"},
                {'name': 'customers', 'query': "SELECT * FROM customers"},
                {'name': 'collections', 'query': "SELECT * FROM collections"},
                {'name': 'other_payments', 'query': "SELECT * FROM other_payments"},
                {'name': 'validation', 'query': "SELECT * FROM validation"},
                {'name': 'resolutions', 'query': "SELECT * FROM resolutions"},
                {'name': 'discounts', 'query': "SELECT * FROM discounts"},
                {'name': 'adjustments', 'query': "SELECT * FROM adjustments"}
            ]
            
            # Filter by subset if provided
            tables_to_pull = all_configs if table_subset is None else [c for c in all_configs if c['name'] in table_subset]
            
            total_tables = len(tables_to_pull)
            for i, config in enumerate(tables_to_pull):
                if self._stop_event.is_set(): break
                
                table = config['name']
                base_query = config['query']
                perc = int((i / total_tables) * 100)
                
                if progress_callback:
                    progress_callback(table, perc, f"Syncing {table}...", self._get_eta(perc))
                
                try:
                    # 1. Determine local high-water mark (Max ID)
                    with self.local_engine.connect() as local_conn:
                        # SQLite column check
                        res_cols = local_conn.execute(text(f"PRAGMA table_info({table})"))
                        all_local_rows = res_cols.fetchall()
                        local_cols = [r[1] for r in all_local_rows]
                        has_id = 'id' in local_cols
                        
                        if has_id:
                            res = local_conn.execute(text(f"SELECT MAX(id) FROM {table}"))
                            max_id = res.scalar() or 0
                        else:
                            max_id = -1 # Full sync
                        
                    # 2. Build incremental query
                    # If we have an id column and it's not a full sync, use WHERE id > max_id
                    inc_query = base_query
                    if has_id and max_id >= 0:
                        if "WHERE" in base_query.upper():
                            inc_query += f" AND id > {max_id}"
                        else:
                            inc_query += f" WHERE id > {max_id}"
                    
                    # 3. Fetch rows from cloud
                    with self.cloud_engine.connect() as cloud_conn:
                        chunk_list = []
                        for chunk in pd.read_sql_query(inc_query, cloud_conn, chunksize=5000):
                            if self._stop_event.is_set(): break
                            chunk_list.append(chunk)
                        
                        if self._stop_event.is_set() or not chunk_list:
                            continue
                            
                        df = pd.concat(chunk_list)
                        
                    if not df.empty:
                        # SQLite Compatibility: Convert all datetime columns to strings
                        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                            df[col] = df[col].astype(str)
                        
                        # Encrypt sensitive fields if pulling 'staff' table
                        if table == 'staff':
                            from utils.security import SecurityManager
                            if 'email' in df.columns:
                                df['email'] = df['email'].apply(lambda x: SecurityManager.encrypt_data(x) if x else x)
                            if 'phone_number' in df.columns:
                                df['phone_number'] = df['phone_number'].apply(lambda x: SecurityManager.encrypt_data(x) if x else x)
                        
                        # Filter to columns that exist locally
                        df_filtered = df[[c for c in df.columns if c in local_cols]]
                        
                        with self.local_engine.begin() as local_conn:
                            # IMPORTANT: We no longer DELETE. We only append new rows.
                            df_filtered.to_sql(table, con=local_conn, if_exists="append", index=False, chunksize=1000)
                except Exception as table_err:
                    logging.error(f"Error pulling {table}: {table_err}")

            if progress_callback:
                status = "Done" if not self._stop_event.is_set() else "Cancelled"
                progress_callback(status, 100, f"Pull sync {status.lower()}.", 0)
                
        except Exception as e:
            logging.error(f"Error during pull_from_cloud: {e}")
        finally:
            self._sync_lock.release()

