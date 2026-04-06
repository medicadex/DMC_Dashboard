from sqlalchemy import text

class JobFormService:
    def __init__(self, engine):
        self.engine = engine

    def get_tables_and_columns(self):
        with self.engine.connect() as conn:
            tables = [row[0] for row in conn.execute(text("SHOW TABLES"))]
            table_columns = {}
            for table in tables:
                if not table.startswith("staging_"):
                    columns = [row[0] for row in conn.execute(text(f"DESCRIBE `{table}`"))]
                    table_columns[table] = columns
            return table_columns

    def get_distinct_values(self, table, column):
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL ORDER BY {column}"))
            return [r[0] for r in res.fetchall()]

    def get_officer_names(self, bus, otypes):
        with self.engine.connect() as conn:
            placeholders_bu = [f":bu{i}" for i in range(len(bus))]
            placeholders_ot = [f":ot{i}" for i in range(len(otypes))]
            params = {f"bu{i}": b for i, b in enumerate(bus)}
            params.update({f"ot{i}": o for i, o in enumerate(otypes)})
            
            sql = f"SELECT DISTINCT account_officer FROM customers WHERE business_unit IN ({', '.join(placeholders_bu)}) AND officer_type IN ({', '.join(placeholders_ot)}) ORDER BY account_officer"
            res = conn.execute(text(sql), params)
            return [r[0] for r in res.fetchall()]

    def get_feeders(self, bus, names):
        with self.engine.connect() as conn:
            p_bu = [f":bu{i}" for i in range(len(bus))]
            p_nm = [f":n{i}" for i in range(len(names))]
            params = {f"bu{i}": b for i, b in enumerate(bus)}
            params.update({f"n{i}": n for i, n in enumerate(names)})
            
            sql = f"SELECT DISTINCT feeder FROM customers WHERE business_unit IN ({', '.join(p_bu)}) AND account_officer IN ({', '.join(p_nm)}) ORDER BY feeder"
            res = conn.execute(text(sql), params)
            return [r[0] for r in res.fetchall()]

    def get_dt_names(self, bus, feeders):
        with self.engine.connect() as conn:
            p_bu = [f":bu{i}" for i in range(len(bus))]
            p_f = [f":f{i}" for i in range(len(feeders))]
            params = {f"bu{i}": b for i, b in enumerate(bus)}
            params.update({f"f{i}": f for i, f in enumerate(feeders)})
            
            sql = f"SELECT DISTINCT dt_name FROM customers WHERE business_unit IN ({', '.join(p_bu)}) AND feeder IN ({', '.join(p_f)}) ORDER BY dt_name"
            res = conn.execute(text(sql), params)
            return [r[0] for r in res.fetchall()]

    def _get_base_query_parts(self, filters):
        bus = filters.get("bus", [])
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
            LEFT JOIN (SELECT account_number, SUM(discounted_amount) as total_discounts FROM discounts WHERE status = 'approved' GROUP BY account_number) d ON c.account_number = d.account_number
            LEFT JOIN (SELECT account_number, SUM(adjustment_amount) as total_adjustments FROM adjustments WHERE status = 'approved' GROUP BY account_number) a ON c.account_number = a.account_number
        """
        
        pp_cond = """
            CASE 
                WHEN (COALESCE(p.total_payments, 0) >= 0.3 * COALESCE(c.closing_balance, 0)) 
                AND (COALESCE(c.closing_balance, 0) - COALESCE(p.total_payments, 0) - COALESCE(d.total_discounts, 0) + COALESCE(a.total_adjustments, 0) > 0) 
                THEN 'Yes' ELSE 'No' 
            END
        """
        
        if ftype == "Only Payment Plan":
            filter_clauses.append(f"{pp_cond} = 'Yes'")
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
                    COALESCE(d.total_discounts, 0) as total_discounts_valid,
                    COALESCE(a.total_adjustments, 0) as total_adjustments_valid
                FROM customers c 
                {joins}
                {where_sql}
            """
            df = pd.read_sql(text(sql), conn, params=params)
            
            if df.empty:
                return pd.DataFrame()

            val_df = pd.read_sql(text("SELECT account_number, phone_number FROM validation"), conn)
            val_df = val_df.sort_values('account_number').groupby('account_number').last().reset_index()
            df = df.merge(val_df, on='account_number', how='left')

            df['outstanding_balance'] = df['closing_balance'].fillna(0) - df['total_payments'] - df['total_discounts_valid'] - df['total_adjustments_valid']
            df['payment_plan'] = df.apply(lambda r: 'Yes' if (r['total_payments'] >= 0.3 * (r['closing_balance'] or 0)) and (r['outstanding_balance'] > 0) else 'No', axis=1)

            final_cols = [c for c in out_cols if c in df.columns]
            return df[final_cols]
