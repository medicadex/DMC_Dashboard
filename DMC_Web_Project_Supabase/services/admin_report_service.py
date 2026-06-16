import pandas as pd
from sqlalchemy import text
from datetime import datetime
import os

class AdminReportService:
    def __init__(self, engine, staff_repo):
        self.engine = engine
        self.repo = staff_repo

    def get_resolution_report(self):
        """
        Query the MySQL resolution table to identify all customer accounts marked as resolved.
        Calculate totals and breakdown statistics.
        """
        try:
            with self.engine.connect() as conn:
                # 1. Total resolved accounts (exclude 'nan' or empty resolution)
                res_count_sql = """
                    SELECT COUNT(DISTINCT account_number) 
                    FROM resolutions 
                    WHERE resolution IS NOT NULL 
                    AND resolution != '' 
                    AND resolution != 'nan'
                """
                res_count = conn.execute(text(res_count_sql)).scalar() or 0
                
                # 2. Total unresolved accounts (customers not in resolutions or with 'nan'/empty resolution)
                unres_count_sql = """
                    SELECT COUNT(*) FROM customers c 
                    WHERE NOT EXISTS (
                        SELECT 1 FROM resolutions r 
                        WHERE r.account_number = c.account_number 
                        AND r.resolution IS NOT NULL 
                        AND r.resolution != '' 
                        AND r.resolution != 'nan'
                    )
                """
                unres_count = conn.execute(text(unres_count_sql)).scalar() or 0
                
                # 3. Breakdown by resolution type/status/outcome (exclude 'nan')
                breakdown_sql = """
                    SELECT 
                        resolution as Resolution_Type,
                        outcome_of_actions_taken as Outcome,
                        COUNT(*) as Count
                    FROM resolutions 
                    WHERE resolution IS NOT NULL 
                    AND resolution != '' 
                    AND resolution != 'nan'
                    GROUP BY resolution, outcome_of_actions_taken
                """
                breakdown_df = pd.read_sql(text(breakdown_sql), conn)
                
                return {
                    "resolved_count": res_count,
                    "unresolved_count": unres_count,
                    "breakdown": breakdown_df
                }
        except Exception as e:
            raise Exception(f"Failed to fetch resolution report: {e}")

    def get_validation_report(self, start_date=None, end_date=None, status=None):
        """
        Retrieve validation records and generate analysis.
        """
        try:
            params = {}
            filters = []
            if start_date:
                filters.append("v.validation_date >= :start")
                params["start"] = f"{start_date} 00:00:00"
            if end_date:
                filters.append("v.validation_date <= :end")
                params["end"] = f"{end_date} 23:59:59"
            if status:
                filters.append("v.physical_status = :status")
                params["status"] = status
            
            where_clause = " WHERE " + " AND ".join(filters) if filters else ""
            
            # Join with customers to get the latest names if they were updated
            sql = f"""
                SELECT 
                    v.*, 
                    COALESCE(c.account_name, v.account_name) as display_name
                FROM validation v
                LEFT JOIN customers c ON v.account_number = c.account_number
                {where_clause}
                ORDER BY v.validation_date DESC
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn, params=params)
                
                if df.empty:
                    return {
                        "total_validated": 0,
                        "status_distribution": pd.DataFrame(columns=['Status', 'Count']),
                        "time_trends": pd.DataFrame(columns=['Date', 'Count']),
                        "data": df
                    }
                
                # Total validated
                total_validated = len(df)
                
                # Status distribution
                status_dist = df['physical_status'].value_counts().reset_index()
                status_dist.columns = ['Status', 'Count']
                
                # Time trends (by day)
                df['v_date'] = pd.to_datetime(df['validation_date']).dt.date
                time_trends = df.groupby('v_date').size().reset_index()
                time_trends.columns = ['Date', 'Count']
                
                return {
                    "total_validated": total_validated,
                    "status_distribution": status_dist,
                    "time_trends": time_trends,
                    "data": df
                }
        except Exception as e:
            raise Exception(f"Failed to fetch validation report: {e}")


    def get_disconnection_report(self, start_date=None, end_date=None):
        """
        Generate disconnection report.
        """
        try:
            params = {}
            filters = []
            if start_date:
                filters.append("disconnection_date >= :start")
                params["start"] = f"{start_date} 00:00:00"
            if end_date:
                filters.append("disconnection_date <= :end")
                params["end"] = f"{end_date} 23:59:59"
            
            where_clause = " WHERE " + " AND ".join(filters) if filters else ""
            sql = f"SELECT * FROM disconnections{where_clause} ORDER BY disconnection_date DESC"
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn, params=params)
                return df
        except Exception as e:
            raise Exception(f"Failed to fetch disconnection report: {e}")

    def validate_excel_report(self, report_type, file_path, username):
        """
        Validate uploaded Excel reports.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError("Excel file not found.")
            
        try:
            df = pd.read_excel(file_path)
            errors = []
            valid_records = []
            
            if report_type == "resolution":
                return self._validate_resolution_report(df, username)
            elif report_type == "disconnection":
                return self._validate_disconnection_report(df, username)
            elif report_type == "migration":
                return self._validate_migration_report(df, username)
            else:
                raise ValueError(f"Unknown report type: {report_type}")
                
        except Exception as e:
            raise Exception(f"Validation failed: {e}")

    def _validate_resolution_report(self, df, username):
        """
        Resolution Report Validation:
        - Duplication check
        - Mandatory field validation
        - Conditional validation
        """
        errors = []
        valid_rows = []
        
        # Required columns
        required_cols = ['account_number', 'resolution', 'outcome_of_actions_taken']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

        with self.engine.connect() as conn:
            # Pre-fetch existing account numbers for duplication check
            existing_accounts = set(r[0] for r in conn.execute(text("SELECT account_number FROM resolutions")).fetchall())

            for idx, row in df.iterrows():
                row_errors = []
                acc_num = row.get('account_number')
                res_val = str(row.get('resolution')).lower().strip()
                outcome = str(row.get('outcome_of_actions_taken')).strip()

                # 1. Duplication check
                if acc_num in existing_accounts:
                    row_errors.append("Account already exists in resolution records.")
                
                # 2. Mandatory field validation
                allowed_res = ["collectible debt", "uncollectable debt"]
                if res_val not in allowed_res:
                    row_errors.append(f"Resolution must be one of: {', '.join(allowed_res)}")
                
                # 3. Conditional validation
                if res_val == "collectible debt" and (not outcome or outcome.lower() == 'nan'):
                    row_errors.append("Outcome of Resolution is required for 'collectible debt'.")

                if row_errors:
                    errors.append({"row": idx + 2, "account": acc_num, "errors": row_errors})
                else:
                    valid_rows.append(row)
                    
        # Log activity
        self.repo.log_activity(username, "VALIDATION_REPORT", f"Type: Resolution, File: {len(df)} rows, Errors: {len(errors)}", event_type='MAJOR')
        
        return {"total": len(df), "valid": len(valid_rows), "errors": errors, "valid_data": pd.DataFrame(valid_rows)}

    def _validate_disconnection_report(self, df, username):
        """
        Disconnection Report Validation:
        - Check required fields
        - Validate against existing customer records
        """
        errors = []
        valid_rows = []
        
        required_cols = ['account_number', 'disconnection_date', 'reason']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

        with self.engine.connect() as conn:
            # Pre-fetch customer account numbers
            customer_accounts = set(r[0] for r in conn.execute(text("SELECT account_number FROM customers")).fetchall())

            for idx, row in df.iterrows():
                row_errors = []
                acc_num = row.get('account_number')
                
                # 1. Required fields and format
                if not acc_num or str(acc_num).lower() == 'nan':
                    row_errors.append("Account number is missing.")
                
                # 2. Validate against existing customers
                if acc_num not in customer_accounts:
                    row_errors.append("Account number not found in customer records.")

                if row_errors:
                    errors.append({"row": idx + 2, "account": acc_num, "errors": row_errors})
                else:
                    valid_rows.append(row)
        
        self.repo.log_activity(username, "VALIDATION_REPORT", f"Type: Disconnection, File: {len(df)} rows, Errors: {len(errors)}", event_type='MAJOR')
        return {"total": len(df), "valid": len(valid_rows), "errors": errors, "valid_data": pd.DataFrame(valid_rows)}

    def _validate_migration_report(self, df, username):
        """
        Migration Report Validation:
        - Verify data consistency with migration table schema
        - Check for duplicate migration entries
        """
        errors = []
        valid_rows = []
        
        # Assuming migration table has account_number and migration_status
        required_cols = ['account_number', 'migration_status']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

        # For now, we'll check against migration_meta or just duplicates in the file
        seen_accounts = set()
        
        for idx, row in df.iterrows():
            row_errors = []
            acc_num = row.get('account_number')
            
            if acc_num in seen_accounts:
                row_errors.append("Duplicate entry in the same file.")
            seen_accounts.add(acc_num)
            
            if row_errors:
                errors.append({"row": idx + 2, "account": acc_num, "errors": row_errors})
            else:
                valid_rows.append(row)

        self.repo.log_activity(username, "VALIDATION_REPORT", f"Type: Migration, File: {len(df)} rows, Errors: {len(errors)}", event_type='MAJOR')
        return {"total": len(df), "valid": len(valid_rows), "errors": errors, "valid_data": pd.DataFrame(valid_rows)}

    def get_unresolved_resolutions(self, limit=50, offset=0):
        """
        Identify and export accounts where resolution is 'nan' or missing.
        Includes full demographic data.
        """
        try:
            sql = f"""
                SELECT 
                    c.account_number, c.account_name, c.account_address, 
                    c.business_unit, c.undertaking, c.feeder, c.dt_name, 
                    c.account_officer, c.officer_type
                FROM customers c
                WHERE NOT EXISTS (
                    SELECT 1 FROM resolutions r 
                    WHERE r.account_number = c.account_number 
                    AND r.resolution IS NOT NULL 
                    AND r.resolution != '' 
                    AND r.resolution != 'nan'
                )
            """
            
            with self.engine.connect() as conn:
                # Get total count
                count_sql = f"SELECT COUNT(*) FROM ({sql}) as t"
                total_count = conn.execute(text(count_sql)).scalar()
                
                # Get paginated data
                data_sql = f"{sql} LIMIT {limit} OFFSET {offset}"
                df = pd.read_sql(text(data_sql), conn)
                
                return {"total": total_count, "data": df}
        except Exception as e:
            raise Exception(f"Failed to fetch unresolved resolutions: {e}")

    def get_unvalidated_accounts(self, limit=50, offset=0):
        """
        Identify and export accounts that are NOT in the validation table.
        This represents 'Pending Validations'.
        """
        try:
            # Query to find accounts in customers that have NO entry in the validation table
            sql = f"""
                SELECT 
                    c.account_number, c.account_name, c.account_address, 
                    c.business_unit, c.undertaking, c.feeder, c.dt_name, 
                    c.account_officer, c.officer_type
                FROM customers c
                WHERE NOT EXISTS (
                    SELECT 1 FROM validation v 
                    WHERE v.account_number = c.account_number
                )
            """
            
            with self.engine.connect() as conn:
                # Get total count
                count_sql = f"SELECT COUNT(*) FROM ({sql}) as t"
                total_count = conn.execute(text(count_sql)).scalar()
                
                # Get paginated data
                data_sql = f"{sql} LIMIT {limit} OFFSET {offset}"
                df = pd.read_sql(text(data_sql), conn)
                
                return {"total": total_count, "data": df}
        except Exception as e:
            raise Exception(f"Failed to fetch unvalidated accounts: {e}")

    def export_unresolved_resolutions(self, fpath, username):
        """Export all unresolved resolutions to Excel/CSV."""
        try:
            sql = """
                SELECT 
                    c.account_number, c.account_name, c.account_address, 
                    c.business_unit, c.undertaking, c.feeder, c.dt_name, 
                    c.account_officer, c.officer_type
                FROM customers c
                WHERE NOT EXISTS (
                    SELECT 1 FROM resolutions r 
                    WHERE r.account_number = c.account_number 
                    AND r.resolution IS NOT NULL 
                    AND r.resolution != '' 
                    AND r.resolution != 'nan'
                )
            """
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
                
            if fpath.endswith('.xlsx'):
                df.to_excel(fpath, index=False)
            else:
                df.to_csv(fpath, index=False)
                
            self.repo.log_activity(username, "EXPORT_UNRESOLVED", f"Rows: {len(df)}", event_type='MAJOR')
            return len(df)
        except Exception as e:
            raise Exception(f"Export failed: {e}")

    def export_unvalidated_accounts(self, fpath, username):
        """Export all unvalidated accounts to Excel/CSV."""
        try:
            sql = """
                SELECT 
                    c.account_number, c.account_name, c.account_address, 
                    c.business_unit, c.undertaking, c.feeder, c.dt_name, 
                    c.account_officer, c.officer_type
                FROM customers c
                WHERE NOT EXISTS (
                    SELECT 1 FROM validation v 
                    WHERE v.account_number = c.account_number
                )
            """
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
                
            if fpath.endswith('.xlsx'):
                df.to_excel(fpath, index=False)
            else:
                df.to_csv(fpath, index=False)
                
            self.repo.log_activity(username, "EXPORT_UNVALIDATED", f"Rows: {len(df)}", event_type='MAJOR')
            return len(df)
        except Exception as e:
            raise Exception(f"Export failed: {e}")

