import pandas as pd
from sqlalchemy import text

class ExportService:
    def __init__(self, engine):
        self.engine = engine

    def get_table_names(self):
        with self.engine.connect() as conn:
            return [row[0] for row in conn.execute(text("SHOW TABLES"))]

    def get_table_preview(self, table_name):
        with self.engine.connect() as conn:
            return pd.read_sql(f"SELECT * FROM {table_name} LIMIT 100", conn)

    def get_filtered_table(self, table_name, filters):
        with self.engine.connect() as conn:
            params = {}
            where_clauses = []

            # Global search
            if filters.get("search"):
                q = filters["search"]
                try:
                    res = conn.execute(text(f"DESCRIBE `{table_name}`"))
                    cols = [row[0] for row in res.fetchall()]
                    search_parts = []
                    for col in cols:
                        param_name = f"sq_{col.replace(' ', '_')}"
                        search_parts.append(f"`{col}` LIKE :{param_name}")
                        params[param_name] = f"%{q}%"
                    if search_parts:
                        where_clauses.append(f"({' OR '.join(search_parts)})")
                except Exception as e:
                    print(f"Error describing table {table_name}: {e}")

            # Column-specific filters
            if filters.get("column_filters"):
                for col, val in filters["column_filters"].items():
                    if val and val != col:
                        param_name = f"col_{col.replace(' ', '_')}"
                        where_clauses.append(f"`{col}` LIKE :{param_name}")
                        params[param_name] = f"%{val}%"

            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            # Get total count
            count_sql = f"SELECT COUNT(*) FROM `{table_name}`{where_sql}"
            total = conn.execute(text(count_sql), params).scalar()

            # Get data with limit
            sql = f"SELECT * FROM `{table_name}`{where_sql} LIMIT 1000"
            df = pd.read_sql(text(sql), conn, params=params)

            return df, total

    def export_table(self, table_name, filters, fmt, fpath):
        with self.engine.connect() as conn:
            params = {}
            where_clauses = []

            # Global search
            if filters.get("search"):
                q = filters["search"]
                try:
                    res = conn.execute(text(f"DESCRIBE `{table_name}`"))
                    cols = [row[0] for row in res.fetchall()]
                    search_parts = []
                    for col in cols:
                        param_name = f"sq_{col.replace(' ', '_')}"
                        search_parts.append(f"`{col}` LIKE :{param_name}")
                        params[param_name] = f"%{q}%"
                    if search_parts:
                        where_clauses.append(f"({' OR '.join(search_parts)})")
                except Exception as e:
                    print(f"Error describing table {table_name}: {e}")

            # Column-specific filters
            if filters.get("column_filters"):
                for col, val in filters["column_filters"].items():
                    if val and val != col:
                        param_name = f"col_{col.replace(' ', '_')}"
                        where_clauses.append(f"`{col}` LIKE :{param_name}")
                        params[param_name] = f"%{val}%"

            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            sql = f"SELECT * FROM `{table_name}`{where_sql}"
            
            df = pd.read_sql(text(sql), conn, params=params)
            
            if fmt == 'xlsx': 
                df.to_excel(fpath, index=False)
            else: 
                df.to_csv(fpath, index=False)
            
            return len(df)
