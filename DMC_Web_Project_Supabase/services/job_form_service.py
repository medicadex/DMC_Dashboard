from sqlalchemy import text

class JobFormService:
    def __init__(self, engine):
        self.engine = engine

    def get_tables_and_columns(self):
        with self.engine.connect() as conn:
            tables = [row[0] for row in conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))]
            table_columns = {}
            for table in tables:
                if not table.startswith("staging_"):
                    columns = [row[0] for row in conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"))]
                    table_columns[table] = columns
            return table_columns

    def get_distinct_values(self, table, column):
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL ORDER BY {column}"))
            return [r[0] for r in res.fetchall()]

    def get_officer_names(self, bus, otypes, undertakings=None):
        with self.engine.connect() as conn:
            placeholders_bu = [f":bu{i}" for i in range(len(bus))]
            placeholders_ot = [f":ot{i}" for i in range(len(otypes))]
            params = {f"bu{i}": b for i, b in enumerate(bus)}
            params.update({f"ot{i}": o for i, o in enumerate(otypes)})
            
            undertaking_clause = ""
            if undertakings:
                placeholders_un = [f":un{i}" for i in range(len(undertakings))]
                undertaking_clause = f" AND undertaking IN ({', '.join(placeholders_un)})"
                params.update({f"un{i}": u for i, u in enumerate(undertakings)})
            
            sql = f"SELECT DISTINCT account_officer FROM customers WHERE business_unit IN ({', '.join(placeholders_bu)}){undertaking_clause} AND officer_type IN ({', '.join(placeholders_ot)}) ORDER BY account_officer"
            res = conn.execute(text(sql), params)
            return [r[0] for r in res.fetchall()]

    def get_feeders(self, bus, names, undertakings=None):
        with self.engine.connect() as conn:
            p_bu = [f":bu{i}" for i in range(len(bus))]
            p_nm = [f":n{i}" for i in range(len(names))]
            params = {f"bu{i}": b for i, b in enumerate(bus)}
            params.update({f"n{i}": n for i, n in enumerate(names)})
            
            undertaking_clause = ""
            if undertakings:
                p_un = [f":un{i}" for i in range(len(undertakings))]
                undertaking_clause = f" AND undertaking IN ({', '.join(p_un)})"
                params.update({f"un{i}": u for i, u in enumerate(undertakings)})
            
            sql = f"SELECT DISTINCT feeder FROM customers WHERE business_unit IN ({', '.join(p_bu)}){undertaking_clause} AND account_officer IN ({', '.join(p_nm)}) ORDER BY feeder"
            res = conn.execute(text(sql), params)
            return [r[0] for r in res.fetchall()]

    def get_dt_names(self, bus, feeders, undertakings=None):
        with self.engine.connect() as conn:
            p_bu = [f":bu{i}" for i in range(len(bus))]
            p_f = [f":f{i}" for i in range(len(feeders))]
            params = {f"bu{i}": b for i, b in enumerate(bus)}
            params.update({f"f{i}": f for i, f in enumerate(feeders)})
            
            undertaking_clause = ""
            if undertakings:
                p_un = [f":un{i}" for i in range(len(undertakings))]
                undertaking_clause = f" AND undertaking IN ({', '.join(p_un)})"
                params.update({f"un{i}": u for i, u in enumerate(undertakings)})
            
            sql = f"SELECT DISTINCT dt_name FROM customers WHERE business_unit IN ({', '.join(p_bu)}){undertaking_clause} AND feeder IN ({', '.join(p_f)}) ORDER BY dt_name"
            res = conn.execute(text(sql), params)
            return [r[0] for r in res.fetchall()]

    def get_undertakings(self, bus):
        with self.engine.connect() as conn:
            p_bu = [f":bu{i}" for i in range(len(bus))]
            params = {f"bu{i}": b for i, b in enumerate(bus)}
            sql = f"SELECT DISTINCT undertaking FROM customers WHERE business_unit IN ({', '.join(p_bu)}) AND undertaking IS NOT NULL AND undertaking != '' ORDER BY undertaking"
            res = conn.execute(text(sql), params)
            return [r[0] for r in res.fetchall()]

    def _get_base_query_parts(self, filters):
        bus = filters.get("bus", [])
        undertakings = filters.get("undertakings", [])
        otypes = filters.get("otypes", [])
        onames = filters.get("onames", [])
        feeders = filters.get("feeders", [])
        dts = filters.get("dts", [])
        ftype = filters.get("ftype", "Full")

        params = {}
        filter_clauses = []
        
        if bus:
            p_bu = [f":bu{i}" for i in range(len(bus))]
            filter_clauses.append(f"c.business_unit IN ({', '.join(p_bu)})")
            params.update({f"bu{i}": b for i, b in enumerate(bus)})
        if undertakings:
            p_un = [f":un{i}" for i in range(len(undertakings))]
            filter_clauses.append(f"c.undertaking IN ({', '.join(p_un)})")
            params.update({f"un{i}": u for i, u in enumerate(undertakings)})
        if otypes:
            p_ot = [f":ot{i}" for i in range(len(otypes))]
            filter_clauses.append(f"c.officer_type IN ({', '.join(p_ot)})")
            params.update({f"ot{i}": o for i, o in enumerate(otypes)})
        if onames:
            p_n = [f":n{i}" for i in range(len(onames))]
            filter_clauses.append(f"c.account_officer IN ({', '.join(p_n)})")
            params.update({f"n{i}": n for i, n in enumerate(onames)})
        if feeders:
            f_marks = [f":f{i}" for i in range(len(feeders))]
            filter_clauses.append(f"c.feeder IN ({', '.join(f_marks)})")
            params.update({f"f{i}": f for i, f in enumerate(feeders)})
        if dts:
            dt_marks = [f":d{i}" for i in range(len(dts))]
            filter_clauses.append(f"c.dt_name IN ({', '.join(dt_marks)})")
            params.update({f"d{i}": d for i, d in enumerate(dts)})

        joins = """
            LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments GROUP BY account_number) p ON c.account_number = p.account_number
            LEFT JOIN (SELECT account_number, MAX(date_of_payment) as last_payment_date FROM collections GROUP BY account_number) coll ON c.account_number = coll.account_number
            LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_other FROM other_payments GROUP BY account_number) o ON c.account_number = o.account_number
            LEFT JOIN (
                SELECT account_number, 
                    SUM(CASE WHEN LOWER(status) != 'rejected' THEN discounted_amount ELSE 0 END) as total_discounts_display,
                    SUM(CASE WHEN LOWER(status) = 'approved' OR LOWER(user_who_approved) LIKE '%okoye%' OR LOWER(user_who_approved) LIKE '%forstinus%' THEN discounted_amount ELSE 0 END) as total_discounts_approved
                FROM discounts GROUP BY account_number
            ) d ON c.account_number = d.account_number
            LEFT JOIN (
                SELECT account_number, 
                    SUM(CASE WHEN LOWER(status) != 'rejected' THEN adjustment_amount ELSE 0 END) as total_adjustments_display,
                    SUM(CASE WHEN LOWER(status) = 'approved' OR LOWER(user_who_approved_adjustment) LIKE '%okoye%' OR LOWER(user_who_approved_adjustment) LIKE '%forstinus%' THEN adjustment_amount ELSE 0 END) as total_adjustments_approved
                FROM adjustments GROUP BY account_number
            ) a ON c.account_number = a.account_number
        """
        
        pp_cond = """
            CASE 
                WHEN (COALESCE(p.total_payments, 0) >= 0.3 * COALESCE(c.closing_balance, 0)) 
                AND (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts_approved, 0) - COALESCE(a.total_adjustments_approved, 0) > 0) 
                THEN 'Yes' ELSE 'No' 
            END
        """
        
        if ftype == "Defaulted payment Plan":
            filter_clauses.append(f"{pp_cond} = 'Yes'")
            filter_clauses.append("(coll.last_payment_date IS NULL OR (CURRENT_DATE - coll.last_payment_date::date) > 30)")
        elif ftype == "Exclude Payment Plan":
            filter_clauses.append(f"{pp_cond} = 'No'")
            
        return filter_clauses, joins, params

    def count_job_form_rows(self, filters):
        with self.engine.connect() as conn:
            filter_clauses, joins, params = self._get_base_query_parts(filters)
            where_sql = f"WHERE {' AND '.join(filter_clauses)}" if filter_clauses else ""
            sql = f"SELECT COUNT(*) FROM customers c {joins} {where_sql}"
            return conn.execute(text(sql), params).scalar()

    def get_job_form_data(self, filters, out_cols):
        import pandas as pd
        with self.engine.connect() as conn:
            filter_clauses, joins, params = self._get_base_query_parts(filters)
            where_sql = f"WHERE {' AND '.join(filter_clauses)}" if filter_clauses else ""
            sql = f"""
                SELECT 
                    c.*, 
                    COALESCE(p.total_payments, 0) as total_payments,
                    COALESCE(o.total_other, 0) as total_other,
                    COALESCE(d.total_discounts_display, 0) as discount,
                    COALESCE(d.total_discounts_approved, 0) as discount_approved,
                    COALESCE(a.total_adjustments_display, 0) as adjustment,
                    COALESCE(a.total_adjustments_approved, 0) as adjustment_approved,
                    coll.last_payment_date
                FROM customers c 
                {joins}
                {where_sql}
            """
            df = pd.read_sql(text(sql), conn, params=params)
            
            if df.empty:
                return pd.DataFrame()

            # Optimization: Fetch validation data ONLY for the accounts in the current result set
            acc_list = df['account_number'].unique().tolist()
            if acc_list:
                # Use parameterized query for safety and performance
                val_sql = text("SELECT account_number, phone_number FROM validation WHERE account_number = ANY(:accs)")
                val_df = pd.read_sql(val_sql, conn, params={"accs": acc_list})
                if not val_df.empty:
                    val_df = val_df.sort_values('account_number').groupby('account_number').last().reset_index()
                    df = df.merge(val_df, on='account_number', how='left')
                else:
                    df['phone_number'] = None
            else:
                df['phone_number'] = None

            # Dynamic Outstanding Balance matches dashboard model (Using approved only)
            df['outstanding_balance'] = df['closing_balance'].fillna(0) - df['total_payments'] - df['discount_approved'] - df['adjustment_approved']
            
            # Consolidated Payments for Display mapped to pos_other_payments
            df['pos_other_payments'] = df['total_payments'] + df['total_other']
            
            # Payment Plan (Yes/No) matches dashboard model (Using approved only)
            df['payment_plan'] = df.apply(lambda r: 'Yes' if (r['total_payments'] >= 0.3 * (r['closing_balance'] or 0)) and (r['outstanding_balance'] > 0) else 'No', axis=1)

            # Payment Plan Status (Active/Defaulted/No Plan)
            def calculate_pp_status(row):
                if row['payment_plan'] == 'No':
                    return "No Plan"
                
                last_pay = row['last_payment_date']
                if pd.isna(last_pay):
                    return "Defaulted"
                
                last_pay_ts = pd.to_datetime(last_pay)
                if pd.isna(last_pay_ts):
                    return "Defaulted"
                
                today = pd.Timestamp.now().normalize()
                diff = (today - last_pay_ts.normalize()).days
                return "Active" if diff <= 30 else "Defaulted"

            df['payment_plan_status'] = df.apply(calculate_pp_status, axis=1)

            final_cols = [c for c in out_cols if c in df.columns]
            return df[final_cols]
