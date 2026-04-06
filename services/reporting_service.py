from sqlalchemy import text
import pandas as pd # type: ignore
import os
from datetime import datetime, timedelta
import numpy as np # type: ignore
import holidays # type: ignore
from pandas.tseries.offsets import BDay
from typing import Any, List, Dict, Optional
from db_utils import is_online, get_local_engine # type: ignore

class ReportingService:
    def __init__(self, engine, staff_repo):
        self.engine = engine
        self.local_engine = get_local_engine()
        self.repo = staff_repo
        self.ng_holidays = holidays.NG()

    def get_working_days(self, start_date, end_date):
        """Calculates working days excluding weekends and Nigerian public holidays."""
        business_days = pd.bdate_range(start_date, end_date)
        working_days = [d for d in business_days if d.date() not in self.ng_holidays]
        return len(working_days)

    def get_performance_metrics(self, end_date, bu_filter=None, off_type="All", search_query=""):
        """
        Implements advanced variance-analysis and performance metrics with dynamic filtering and global search.
        """
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        month_start = end_dt.replace(day=1)
        
        # 1. Prorated MTD Calculations
        total_working_days = self.get_working_days(month_start, (month_start + pd.offsets.MonthEnd(0)))
        elapsed_working_days = self.get_working_days(month_start, end_dt)
        remaining_working_days = max(1, total_working_days - elapsed_working_days)

        # 2. Previous Month-to-Date (MoM) range
        last_month_end = month_start - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        try:
            prev_month_end_day = last_month_start.replace(day=end_dt.day)
        except ValueError:
            prev_month_end_day = last_month_end

        # 3. Dynamic Filter Construction
        filter_clause = ""
        params = {
            "mtd_start": month_start.strftime('%Y-%m-%d 00:00:00'),
            "mtd_end": end_dt.strftime('%Y-%m-%d 23:59:59'),
            "prev_start": last_month_start.strftime('%Y-%m-%d 00:00:00'),
            "prev_end": prev_month_end_day.strftime('%Y-%m-%d 23:59:59')
        }

        if off_type != "All":
            filter_clause += " AND c.officer_type = :off_type"
            params["off_type"] = off_type
            
        if bu_filter and "All" not in bu_filter:
            in_markers = [f":bu_{i}" for i in range(len(bu_filter))]
            for i, bu in enumerate(bu_filter): params[f"bu_{i}"] = bu
            filter_clause += f" AND c.business_unit IN ({', '.join(in_markers)})"
            
        # Apply Global Search (Backend)
        if search_query:
            filter_clause += " AND (c.account_officer LIKE :sq OR c.business_unit LIKE :sq)"
            params["sq"] = f"%{search_query}%"

        active_engine = self.engine if is_online() else self.local_engine
        with active_engine.connect() as conn:
            # 4. Fetch Config Targets
            targets_res = conn.execute(text("SELECT bu_name, monthly_target FROM performance_config")).fetchall()
            bu_targets = {r[0]: float(r[1]) for r in targets_res}
            default_target = 10000000.00

            # 5. Identify Peak Recovery Month (Highest historical month)
            peak_sql = """
                SELECT DATE_FORMAT(date_of_payment, '%Y-%m-01') as month, SUM(amount_paid) as total
                FROM all_payments
                GROUP BY month
                ORDER BY total DESC
                LIMIT 1
            """
            peak_month_res = conn.execute(text(peak_sql)).fetchone()
            peak_month_start = datetime.strptime(peak_month_res[0], '%Y-%m-%d') if peak_month_res else month_start
            
            try:
                peak_period_end = peak_month_start.replace(day=end_dt.day)
            except ValueError:
                peak_offset = peak_month_start + pd.offsets.MonthEnd(0)
                peak_period_end = peak_offset.date() if hasattr(peak_offset, 'date') else peak_offset

            params["peak_start"] = peak_month_start.strftime('%Y-%m-%d 00:00:00')
            params["peak_end"] = peak_period_end.strftime('%Y-%m-%d 23:59:59')

            # 6. Main Performance Query
            # Dynamic grouping: If Vendor, aggregate across BUs
            group_cols = "c.account_officer" if off_type == "Vendor" else "c.business_unit, c.account_officer"
            bu_col = "GROUP_CONCAT(DISTINCT c.business_unit SEPARATOR ', ') as business_unit" if off_type == "Vendor" else "c.business_unit"
            join_on = "curr.account_officer = peak.account_officer" if off_type == "Vendor" else "curr.business_unit = peak.business_unit AND curr.account_officer = peak.account_officer"
            join_on_prev = "curr.account_officer = prev.account_officer" if off_type == "Vendor" else "curr.business_unit = prev.business_unit AND curr.account_officer = prev.account_officer"

            sql = f"""
                WITH current_recovery AS (
                    SELECT 
                        {bu_col}, 
                        c.account_officer,
                        SUM(p.amount_paid) as actual_mtd,
                        COUNT(DISTINCT p.account_number) as response_count,
                        COUNT(DISTINCT CASE WHEN afs.outstanding_balance <= 0 THEN p.account_number END) as payoff_count
                    FROM all_payments p
                    JOIN customers c ON p.account_number = c.account_number
                    LEFT JOIN account_financial_summary afs ON p.account_number = afs.account_number
                    WHERE DATE(p.date_of_payment) BETWEEN DATE(:mtd_start) AND DATE(:mtd_end)
                    {filter_clause}
                    GROUP BY {group_cols}
                ),
                peak_recovery AS (
                    SELECT 
                        {bu_col},
                        c.account_officer,
                        SUM(p.amount_paid) as peak_mtd
                    FROM all_payments p
                    JOIN customers c ON p.account_number = c.account_number
                    WHERE DATE(p.date_of_payment) BETWEEN DATE(:peak_start) AND DATE(:peak_end)
                    {filter_clause}
                    GROUP BY {group_cols}
                ),
                prev_month_recovery AS (
                    SELECT 
                        {bu_col},
                        c.account_officer,
                        SUM(p.amount_paid) as prev_mtd
                    FROM all_payments p
                    JOIN customers c ON p.account_number = c.account_number
                    WHERE DATE(p.date_of_payment) BETWEEN DATE(:prev_start) AND DATE(:prev_end)
                    {filter_clause}
                    GROUP BY {group_cols}
                )
                SELECT 
                    curr.business_unit as BU,
                    curr.account_officer as `Account Officer`,
                    curr.actual_mtd as Actual,
                    curr.response_count as Response,
                    curr.payoff_count as `Debt Pay-Off`,
                    peak.peak_mtd as `Peak Recovery`,
                    prev.prev_mtd as `Prev MoM Recovery`
                FROM current_recovery curr
                LEFT JOIN peak_recovery peak ON {join_on}
                LEFT JOIN prev_month_recovery prev ON {join_on_prev}
            """
            
            df = pd.read_sql(text(sql), conn, params=params)

        # 6. Apply Derived Business Logic
        def calculate_row_metrics(row):
            bu = row['BU']
            monthly_target = bu_targets.get(bu, default_target)
            actual = row['Actual']
            peak_mtd = row['Peak Recovery'] or 0
            prev_mtd = row['Prev MoM Recovery'] or 0
            
            # Prorated Target
            daily_target = monthly_target / total_working_days
            prorated_target = daily_target * elapsed_working_days
            
            # Peak Variances
            peak_variance_abs = actual - peak_mtd
            peak_variance_pct = (peak_variance_abs / peak_mtd * 100) if peak_mtd > 0 else 0
            
            # MoM Variances
            mom_variance_abs = actual - prev_mtd
            mom_variance_pct = (mom_variance_abs / prev_mtd * 100) if prev_mtd > 0 else 0
            
            target_variance_abs = actual - prorated_target
            
            # Run-rate
            daily_runrate = (monthly_target - actual) / remaining_working_days
            
            return pd.Series({
                'Target': monthly_target,
                '% Recovery': (actual / monthly_target * 100) if monthly_target > 0 else 0,
                'Peak Recovery': peak_mtd,
                'Peak Variance (abs)': peak_variance_abs,
                'Peak Variance (%)': peak_variance_pct,
                'Prev MoM Recovery': prev_mtd,
                'MoM Variance (abs)': mom_variance_abs,
                'MoM Variance (%)': mom_variance_pct,
                'Prorated MTD Target': prorated_target,
                'Target Variance (abs)': target_variance_abs,
                'Daily RunRate': max(0.0, float(daily_runrate))
            })

        metrics_df = df.apply(calculate_row_metrics, axis=1)
        # Drop redundant columns before concat if they exist in df
        cols_to_drop = [c for c in metrics_df.columns if c in df.columns and c not in ['BU', 'Account Officer', 'Actual', 'Response', 'Debt Pay-Off']]
        df = df.drop(columns=cols_to_drop)
        df = pd.concat([df, metrics_df], axis=1)
        
        # 7. Global Ranking
        df['Rank'] = df['Actual'].rank(ascending=False, method='min').astype(int)
        
        # Column Ordering per Requirement
        ordered_cols = [
            'BU', 'Account Officer', 'Target', 'Actual', '% Recovery',
            'Peak Recovery', 'Peak Variance (abs)', 'Peak Variance (%)',
            'Prev MoM Recovery', 'MoM Variance (abs)', 'MoM Variance (%)',
            'Prorated MTD Target', 'Target Variance (abs)', 'Daily RunRate',
            'Rank', 'Response', 'Debt Pay-Off'
        ]
        return df[ordered_cols]

    def get_report_data(self, report_type, start_date, end_date, off_type, selected_bus, username, role, mode='listing', limit=1000, offset=0, search_query="", col_filters=None):
        """Reconstructed exact legacy logic from export_table.py with Backend Search and Date Optimization. 
        Renamed 'Detailed Listing' to 'Payment Listing' in documentation."""
        
        # 1. Date Sanitization & Optimization
        params = {
            "start": f"{start_date} 00:00:00",
            "end": f"{end_date} 23:59:59",
            "limit": limit,
            "offset": offset
        }
        
        filters = ""
        
        # Mode-specific filter handling
        if mode == "collection":
            filters = " WHERE 1=1" 
        else:
            filters = " WHERE main.date_of_payment BETWEEN :start AND :end"
        
        if off_type != "All":
            filters = str(filters) + " AND c.officer_type = :off_type"
            params["off_type"] = off_type
            
        if selected_bus and "All" not in selected_bus:
            in_markers: list[Any] = []
            for i, bu in enumerate(selected_bus):
                marker = f"bu_{i}"
                in_markers.append(f":{marker}")
                params[marker] = bu
            filters = str(filters) + f" AND c.business_unit IN ({', '.join(in_markers)})"

        # Backend Search Logic (Global Search)
        if search_query:
            if mode == "collection":
                search_clause = " AND (c.account_number LIKE :sq OR c.account_name LIKE :sq OR c.business_unit LIKE :sq OR c.account_officer LIKE :sq)"
            else:
                search_clause = " AND (main.account_number LIKE :sq OR c.account_name LIKE :sq OR c.business_unit LIKE :sq OR c.account_officer LIKE :sq)"
            filters = str(filters) + search_clause
            params["sq"] = f"%{search_query}%"

        # Apply Column Filters (Backend)
        if col_filters:
            for col, val in col_filters.items():
                if val and val.lower() != col.lower():
                    db_col = col
                    if col == "account_name": db_col = "c.account_name"
                    elif col == "account_number": db_col = "c.account_number" if mode == "collection" else "main.account_number"
                    elif col == "business_unit": db_col = "c.business_unit"
                    elif col == "account_officer": db_col = "COALESCE(h.account_officer, c.account_officer)"
                    
                    param_name = f"col_p_{abs(hash(col))}"
                    filters = str(filters) + f" AND {db_col} LIKE :{param_name}"
                    params[param_name] = f"%{val}%"

        if mode == "listing":
            sql = f"""
                SELECT 
                    main.account_number as `Account Number`, 
                    c.account_name as `Account Name`,
                    c.business_unit as `Business Unit`, 
                    c.undertaking as `Undertaking`,
                    c.dt_name as `DT Name`,
                    c.closing_balance as `Closing Balance`,
                    main.amount_paid as `Amount Paid`,
                    main.date_of_payment as `Date of Payment`, 
                    COALESCE(afs.total_discounts, 0) as `Total Discount`, 
                    COALESCE(afs.total_adjustments, 0) as `Total Adjustment`, 
                    (SELECT SUM(op.amount_paid) FROM other_payments op WHERE op.account_number = main.account_number AND DATE(op.date_of_payment) BETWEEN DATE(:start) AND DATE(:end)) as `Other Payment`,
                    COALESCE(afs.outstanding_balance, 0) as `Outstanding Balance`,
                    COALESCE(afs.payment_plan, 'No') as `Payment Plan (Yes/No)`,
                    COALESCE(h.account_officer, c.account_officer) as `Account Officer`
                FROM all_payments main
                LEFT JOIN customers c ON main.account_number = c.account_number
                LEFT JOIN account_financial_summary afs ON afs.account_number = main.account_number
                LEFT JOIN customer_officer_history h ON main.account_number = h.account_number
                    AND main.date_of_payment BETWEEN h.start_date AND COALESCE(h.end_date, '9999-12-31')
                {filters}
                ORDER BY main.date_of_payment DESC
            """
        elif mode == "performance":
            # Recalculate full set for performance metrics, but we can limit the return if needed
            # For now, performance mode usually returns a summarized small set, so pagination might be less critical 
            # but we'll maintain consistency.
            df = self.get_performance_metrics(end_date, bu_filter=selected_bus, off_type=off_type, search_query=search_query)
            # Apply limit/offset manually to dataframe for performance mode
            total = len(df)
            df = df.iloc[offset : offset + limit]
            return df, total
        elif mode == "collection":
            sql = f"""
                SELECT 
                    c.account_number as `Account Number`,
                    c.account_name as `Account Name`,
                    c.business_unit as `Business Unit`,
                    c.undertaking as `Undertaking`,
                    c.account_officer as `Account Officer`,
                    c.closing_balance as `Closing Balance`,
                    COALESCE(p.total_payments, 0) as `Total Payments`,
                    COALESCE(d.total_discounts, 0) as `Valid Discount Amount`,
                    COALESCE(a.total_adjustments, 0) as `Valid Adjustment Amount`,
                    (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) + COALESCE(a.total_adjustments, 0)) as `Outstanding Balance`,
                    CASE 
                        WHEN (COALESCE(p.total_payments, 0) >= 0.3 * COALESCE(c.closing_balance, 0)) 
                        AND (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) + COALESCE(a.total_adjustments, 0) > 0) 
                        THEN 'Yes' ELSE 'No' 
                    END as `Current Payment-Plan Status`
                FROM customers c
                LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments WHERE DATE(date_of_payment) BETWEEN DATE(:start) AND DATE(:end) GROUP BY account_number) p ON c.account_number = p.account_number
                LEFT JOIN (SELECT account_number, SUM(discounted_amount) as total_discounts FROM discounts WHERE status = 'approved' AND DATE(date_approved) BETWEEN DATE(:start) AND DATE(:end) GROUP BY account_number) d ON c.account_number = d.account_number
                LEFT JOIN (SELECT account_number, SUM(adjustment_amount) as total_adjustments FROM adjustments WHERE status = 'approved' AND DATE(date_approved) BETWEEN DATE(:start) AND DATE(:end) GROUP BY account_number) a ON c.account_number = a.account_number
                {filters}
                ORDER BY c.account_name ASC
            """
        else:
            # Summary Logic
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            params.update({
                "today": end_dt.strftime('%Y-%m-%d'),
                "yesterday": (end_dt - timedelta(days=1)).strftime('%Y-%m-%d'),
                "week_start": (end_dt - timedelta(days=end_dt.weekday())).strftime('%Y-%m-%d'),
                "month_start": end_dt.strftime('%Y-%m-01')
            })

            group_cols = "c.account_officer" if off_type == "Vendor" else "c.business_unit, c.account_officer"
            bu_col = "GROUP_CONCAT(DISTINCT c.business_unit SEPARATOR ', ') as `Business Unit`" if off_type == "Vendor" else "c.business_unit as `Business Unit`"

            sql = f"""
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN DATE(main.date_of_payment) >= :month_start THEN main.amount_paid ELSE 0 END) DESC) as `S/N`,
                    {bu_col},
                    c.account_officer as `Account Officer`,
                    SUM(CASE WHEN DATE(main.date_of_payment) = :today THEN main.amount_paid ELSE 0 END) as `Today's Payment`,
                    SUM(CASE WHEN DATE(main.date_of_payment) = :yesterday THEN main.amount_paid ELSE 0 END) as `Yesterday Recovery`,
                    SUM(CASE WHEN DATE(main.date_of_payment) >= :week_start THEN main.amount_paid ELSE 0 END) as `This Week Recovery`,
                    SUM(CASE WHEN DATE(main.date_of_payment) >= :month_start THEN main.amount_paid ELSE 0 END) as `Total Recovery`
                FROM all_payments main
                LEFT JOIN customers c ON main.account_number = c.account_number
                {filters}
                GROUP BY {group_cols}
                ORDER BY `Total Recovery` DESC
            """

        # Apply limit/offset for preview
        preview_sql = f"SELECT * FROM ({sql}) AS base LIMIT :limit OFFSET :offset"
        
        active_engine = self.engine if is_online() else self.local_engine
        with active_engine.connect() as conn:
            df = pd.read_sql(text(preview_sql), conn, params=params)
            
            # Get total count for summary label
            count_sql = f"SELECT COUNT(*) FROM ({sql}) AS total"
            total_count = conn.execute(text(count_sql), params).scalar()

            if mode == "collection":
                # Compute grand totals globally across the full filtered dataset (bypassing limitation)
                totals_sql = f"""
                    SELECT 
                        SUM(`Closing Balance`), 
                        SUM(`Total Payments`), 
                        SUM(`Outstanding Balance`) 
                    FROM ({sql}) AS t
                """
                t_row = conn.execute(text(totals_sql), params).fetchone()
                if t_row:
                    df.attrs['grand_totals'] = {
                        'Closing Balance': float(t_row[0] or 0),
                        'Total Payments': float(t_row[1] or 0),
                        'Outstanding Balance': float(t_row[2] or 0)
                    }

        return df, total_count

    def get_filter_options(self):
        """Fetches available BU and Officers for filter dropdowns."""
        active_engine = self.engine if is_online() else self.local_engine
        with active_engine.connect() as conn:
            # Use 'customers' table as primary source for filter options
            bus = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE business_unit IS NOT NULL ORDER BY business_unit")).fetchall()
            officers = conn.execute(text("SELECT DISTINCT account_officer FROM customers WHERE account_officer IS NOT NULL ORDER BY account_officer")).fetchall()
            
            return {
                "bus": [r[0] for r in bus],
                "officers": [r[0] for r in officers]
            }

    def export_full_report(self, mode, start_date, end_date, off_type, selected_bus, username, role, filepath, search_query="", col_filters=None):
        """Reconstructed legacy full export logic with filtering and millions formatting."""
        # Get raw data using the logic above but without the LIMIT
        params = {
            "start": f"{start_date} 00:00:00",
            "end": f"{end_date} 23:59:59"
        }
        
        filters = ""
        if mode == "collection":
            filters = " WHERE DATE(c.created_at) BETWEEN DATE(:start) AND DATE(:end)"
        else:
            filters = " WHERE DATE(main.date_of_payment) BETWEEN DATE(:start) AND DATE(:end)"
        
        if off_type != "All":
            filters += " AND c.officer_type = :off_type"
            params["off_type"] = off_type
            
        if selected_bus and "All" not in selected_bus:
            in_markers = [f":bu_{i}" for i in range(len(selected_bus))]
            for i, bu in enumerate(selected_bus): params[f"bu_{i}"] = bu
            filters += f" AND c.business_unit IN ({', '.join(in_markers)})"

        # Apply Global Search (Backend)
        if search_query:
            if mode == "collection":
                filters += " AND (c.account_number LIKE :sq OR c.account_name LIKE :sq OR c.business_unit LIKE :sq OR c.account_officer LIKE :sq)"
            else:
                filters += " AND (main.account_number LIKE :sq OR c.account_name LIKE :sq OR c.business_unit LIKE :sq OR c.account_officer LIKE :sq)"
            params["sq"] = f"%{search_query}%"
            
        # Apply Column Filters (Backend)
        if col_filters:
            for col, val in col_filters.items():
                if val and val.lower() != col.lower():
                    db_col = col
                    if col == "account_name": db_col = "c.account_name"
                    elif col == "account_number": db_col = "c.account_number" if mode == "collection" else "main.account_number"
                    elif col == "business_unit": db_col = "c.business_unit"
                    elif col == "account_officer": db_col = "COALESCE(h.account_officer, c.account_officer)"
                    
                    param_name = f"col_{abs(hash(col))}"
                    filters = str(filters) + f" AND {db_col} LIKE :{param_name}"
                    params[param_name] = f"%{val}%"

        if mode == "listing":
            sql = f"""
                SELECT 
                    main.account_number as `Account Number`, 
                    c.account_name as `Account Name`,
                    c.business_unit as `Business Unit`, 
                    c.undertaking as `Undertaking`,
                    c.dt_name as `DT Name`,
                    c.closing_balance as `Closing Balance`,
                    main.amount_paid as `Amount Paid`,
                    main.date_of_payment as `Date of Payment`, 
                    COALESCE(afs.total_discounts, 0) as `Total Discount`, 
                    COALESCE(afs.total_adjustments, 0) as `Total Adjustment`, 
                    (SELECT SUM(op.amount_paid) FROM other_payments op WHERE op.account_number = main.account_number AND DATE(op.date_of_payment) BETWEEN DATE(:start) AND DATE(:end)) as `Other Payment`,
                    COALESCE(afs.outstanding_balance, 0) as `Outstanding Balance`,
                    COALESCE(afs.payment_plan, 'No') as `Payment Plan (Yes/No)`,
                    COALESCE(h.account_officer, c.account_officer) as `Account Officer`
                FROM all_payments main
                LEFT JOIN customers c ON main.account_number = c.account_number
                LEFT JOIN account_financial_summary afs ON afs.account_number = main.account_number
                LEFT JOIN customer_officer_history h ON main.account_number = h.account_number
                    AND main.date_of_payment BETWEEN h.start_date AND COALESCE(h.end_date, '9999-12-31')
                {filters}
                ORDER BY main.date_of_payment DESC
            """
        elif mode == "performance":
            df = self.get_performance_metrics(end_date, bu_filter=selected_bus, off_type=off_type, search_query=search_query)
        elif mode == "collection":
            sql = f"""
                SELECT 
                    c.account_number as `Account Number`,
                    c.account_name as `Account Name`,
                    c.business_unit as `Business Unit`,
                    c.undertaking as `Undertaking`,
                    c.account_officer as `Account Officer`,
                    c.closing_balance as `Closing Balance`,
                    COALESCE(p.total_payments, 0) as `Total Payments`,
                    COALESCE(d.total_discounts, 0) as `Valid Discount Amount`,
                    COALESCE(a.total_adjustments, 0) as `Valid Adjustment Amount`,
                    (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) + COALESCE(a.total_adjustments, 0)) as `Outstanding Balance`,
                    CASE 
                        WHEN (COALESCE(p.total_payments, 0) >= 0.3 * COALESCE(c.closing_balance, 0)) 
                        AND (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) + COALESCE(a.total_adjustments, 0) > 0) 
                        THEN 'Yes' ELSE 'No' 
                    END as `Current Payment-Plan Status`
                FROM customers c
                LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments WHERE date_of_payment BETWEEN :start AND :end GROUP BY account_number) p ON c.account_number = p.account_number
                LEFT JOIN (SELECT account_number, SUM(discounted_amount) as total_discounts FROM discounts WHERE status = 'approved' AND date_approved BETWEEN :start AND :end GROUP BY account_number) d ON c.account_number = d.account_number
                LEFT JOIN (SELECT account_number, SUM(adjustment_amount) as total_adjustments FROM adjustments WHERE status = 'approved' AND date_approved BETWEEN :start AND :end GROUP BY account_number) a ON c.account_number = a.account_number
                {filters}
                ORDER BY c.account_name ASC
            """
        else:
            # Summary SQL
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            params.update({
                "today": end_dt.strftime('%Y-%m-%d'), 
                "yesterday": (end_dt - timedelta(days=1)).strftime('%Y-%m-%d'), 
                "week_start": (end_dt - timedelta(days=end_dt.weekday())).strftime('%Y-%m-%d'), 
                "month_start": end_dt.strftime('%Y-%m-01')
            })
            
            group_cols = "c.account_officer" if off_type == "Vendor" else "c.business_unit, c.account_officer"
            bu_col = "GROUP_CONCAT(DISTINCT c.business_unit SEPARATOR ', ') as `Business Unit`" if off_type == "Vendor" else "c.business_unit as `Business Unit`"

            sql = f"""
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN DATE(main.date_of_payment) >= :month_start THEN main.amount_paid ELSE 0 END) DESC) as `S/N`,
                    {bu_col}, 
                    c.account_officer as `Account Officer`, 
                    SUM(CASE WHEN DATE(main.date_of_payment) = :today THEN main.amount_paid ELSE 0 END) as `Today's Payment`, 
                    SUM(CASE WHEN DATE(main.date_of_payment) = :yesterday THEN main.amount_paid ELSE 0 END) as `Yesterday Recovery`, 
                    SUM(CASE WHEN DATE(main.date_of_payment) >= :week_start THEN main.amount_paid ELSE 0 END) as `This Week Recovery`, 
                    SUM(CASE WHEN DATE(main.date_of_payment) >= :month_start THEN main.amount_paid ELSE 0 END) as `Total Recovery` 
                FROM all_payments main 
                LEFT JOIN customers c ON main.account_number = c.account_number 
                {filters} 
                GROUP BY {group_cols} 
                ORDER BY `Total Recovery` DESC
            """

        active_engine = self.engine if is_online() else self.local_engine
        with active_engine.connect() as conn:
            if mode != "performance":
                df = pd.read_sql(text(sql), conn, params=params)
            else:
                # For performance mode, recalculate rank on the filtered dataset
                if 'Actual' in df.columns:
                    df['Rank'] = df['Actual'].rank(ascending=False, method='min').astype(int)
                    df = df.sort_values('Rank')

            # Apply "Millions" formatting for performance mode exports if it's Excel
            if mode == "performance":
                amount_cols = ['Target', 'Actual', 'Peak Recovery', 'Peak Variance (abs)', 
                               'Prev MoM Recovery', 'MoM Variance (abs)', 'Prorated MTD Target', 
                               'Target Variance (abs)', 'Daily RunRate']
                df_any: Any = df
                for col in amount_cols:
                    if hasattr(df_any, 'columns') and col in df_any.columns:
                        df_any[col] = df_any[col] / 1000000.0  # type: ignore
                df = df_any

            if filepath.endswith('.xlsx'):
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Report Data', index=False)
                    
                    if mode == "performance":
                        methodology_data = {
                            "Metric": ["Peak Recovery Variance", "MoM Variance Analysis", "Prorated MTD Target", "Daily Run-Rate", "Rank", "Response", "Debt Pay-off"],
                            "Description": ["Compares current recovery against the historical peak month for the same day range.", "Compares current month-to-date recovery with the immediately preceding calendar month for the same day-range.", "Calculates target based on elapsed working days (excluding weekends).", "Amount needed daily to hit monthly target by end of month.", "Rank based on This Month Recovery (descending).", "Unique account numbers that posted any payment within the reporting period.", "Count of Response accounts that now carry a zero balance."],
                            "Formula": ["(Current Recovery - Peak Period Equivalent) / Peak Period Equivalent * 100", "(Current MTD - Prev MTD) / Prev MTD * 100", "(Monthly Target / Total Working Days) * Elapsed Working Days", "(Monthly Target - MTD Actual) / Remaining Working Days", "RANK() OVER (ORDER BY Actual DESC)", "COUNT(DISTINCT account_number)", "COUNT(DISTINCT accounts WHERE balance <= 0)"]
                        }
                        meth_df = pd.DataFrame(methodology_data)
                        meth_df.to_excel(writer, sheet_name='Methodology', index=False)
            else:
                df.to_csv(filepath, index=False)
        
        self.repo.log_activity(username, "EXPORT", f"Report: {os.path.basename(filepath)}", event_type='MAJOR')
        return len(df)
