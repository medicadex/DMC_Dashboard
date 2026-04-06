import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
import os
import json
import threading
from datetime import datetime, timedelta
from cleanup_utils import setup_temp_cleanup
from db_utils import get_db_engine, get_project_folder

# ────────────────────────────────────────────────
# CONFIG & INITIALIZATION
# ────────────────────────────────────────────────
PROJECT_FOLDER = get_project_folder()
setup_temp_cleanup(PROJECT_FOLDER)
CONFIG_FILE = os.path.join(PROJECT_FOLDER, "config.json")

# Load environment variables from .env
# load_dotenv is already called in db_utils

class ExportTool:
    def __init__(self, root, engine=None):
        self.root = root
        
        # Only set title and geometry if root is a TopLevel or Tk window
        if hasattr(self.root, 'title') and not isinstance(self.root, tk.Frame):
            self.root.title("SQL Reporting & Export Tool - Pro Edition")
        
        if hasattr(self.root, 'geometry') and not isinstance(self.root, tk.Frame):
            # Center Window
            self.root.update_idletasks()
            width = 1180
            height = 820
            x = (self.root.winfo_screenwidth() // 2) - (width // 2)
            y = (self.root.winfo_screenheight() // 2) - (height // 2)
            self.root.geometry(f"{width}x{height}+{x}+{y}")
        
        # Data & State
        self.engine = engine if engine else get_db_engine()
        self.current_df = pd.DataFrame()
        self.filtered_df = pd.DataFrame()
        self.column_filters = {}
        self.last_export_path = None
        self._filter_timer = None # For debouncing search
        
        self.setup_styles()
        
        # Main Layout
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill="both", expand=True, padx=15, pady=10)

        # 1. Selection Tabs
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill="x", pady=(0, 10))
        
        self.tab_export = ttk.Frame(self.notebook)
        self.tab_reports = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_export, text=" 📦 Table Export ")
        self.notebook.add(self.tab_reports, text=" 📊 Custom Reports ")
        self.notebook.bind("<<NotebookTabChanged>>", lambda e: self.on_tab_switch())
        
        self.setup_export_tab()
        self.setup_reports_tab()
        
        # 2. Preview Section
        self.setup_preview_section()
        
        # 3. Footer / Status
        self.setup_footer()
        
        # Initial Load
        self.load_config()
        self.refresh_metadata()

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"), background="#E1E1E1")
        style.configure("Treeview", font=("Segoe UI", 9), rowheight=28)
        style.map("Treeview", background=[('selected', '#2B5797')], foreground=[('selected', 'white')])
        
    def setup_export_tab(self):
        container = ttk.Frame(self.tab_export, padding=20)
        container.pack(fill="both")
        
        ttk.Label(container, text="Select Table to Export:", font=("Segoe UI", 11, "bold")).pack(pady=(0, 5))
        self.table_var = tk.StringVar()
        self.table_dropdown = ttk.Combobox(container, textvariable=self.table_var, state="readonly", width=60)
        self.table_dropdown.pack(pady=5)
        
        # Action Buttons
        btn_frame = ttk.Frame(container)
        btn_frame.pack(pady=15)
        
        ttk.Button(btn_frame, text="🔍 Preview Table", width=25, 
                   command=lambda: self.start_bg_task(self.export_table_preview)).grid(row=0, column=0, columnspan=2, pady=10)
        
        ttk.Button(btn_frame, text="📥 Excel (.xlsx)", width=20, 
                   command=lambda: self.start_bg_task(self.export_table_data, 'xlsx')).grid(row=1, column=0, padx=5)
        
        ttk.Button(btn_frame, text="📥 CSV (.csv)", width=20, 
                   command=lambda: self.start_bg_task(self.export_table_data, 'csv')).grid(row=1, column=1, padx=5)

    def setup_reports_tab(self):
        container = ttk.Frame(self.tab_reports, padding=10)
        container.pack(fill="both")
        
        # Left side: Controls
        ctrl_frame = ttk.LabelFrame(container, text=" Parameters ", padding=10)
        ctrl_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))
        
        # Report Mode
        mode_frame = ttk.Frame(ctrl_frame)
        mode_frame.pack(fill="x", pady=5)
        self.report_type_var = tk.StringVar(value="listing")
        ttk.Radiobutton(mode_frame, text="Payment Listing", variable=self.report_type_var, value="listing").pack(side="left", padx=10)
        ttk.Radiobutton(mode_frame, text="Performance Summary", variable=self.report_type_var, value="summary").pack(side="left", padx=10)
        
        # Date Selection
        date_frame = ttk.Frame(ctrl_frame)
        date_frame.pack(fill="x", pady=10)
        
        ttk.Label(date_frame, text="From:").grid(row=0, column=0, padx=5)
        self.start_date_entry = ttk.Entry(date_frame, width=12)
        self.start_date_entry.grid(row=0, column=1, padx=5)
        
        ttk.Label(date_frame, text="To:").grid(row=0, column=2, padx=5)
        self.end_date_entry = ttk.Entry(date_frame, width=12)
        self.end_date_entry.grid(row=0, column=3, padx=5)
        
        # Quick Date Buttons
        qdate_frame = ttk.Frame(ctrl_frame)
        qdate_frame.pack(fill="x", pady=5)
        for label, days in [("Today", 0), ("Last 7 Days", 7), ("Last 30 Days", 30), ("This Month", -1)]:
            ttk.Button(qdate_frame, text=label, width=12, command=lambda d=days: self.set_quick_date(d)).pack(side="left", padx=2)

        # Officer Filter
        off_frame = ttk.Frame(ctrl_frame)
        off_frame.pack(fill="x", pady=10)
        ttk.Label(off_frame, text="Officer Type:").pack(side="left", padx=5)
        self.officer_type_var = tk.StringVar(value="All")
        ttk.OptionMenu(off_frame, self.officer_type_var, "All", "All", "DMO", "Vendor").pack(side="left", padx=5)
        
        # Right side: BU Multi-select
        bu_frame = ttk.LabelFrame(container, text=" Business Units ", padding=10)
        bu_frame.pack(side="right", fill="both", padx=(5, 0))
        
        self.bu_listbox = tk.Listbox(bu_frame, selectmode="multiple", height=8, width=35, font=("Segoe UI", 9))
        self.bu_listbox.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(bu_frame, orient="vertical", command=self.bu_listbox.yview)
        sb.pack(side="right", fill="y")
        self.bu_listbox.config(yscrollcommand=sb.set)
        
        # Report Actions
        act_frame = ttk.Frame(self.tab_reports)
        act_frame.pack(pady=10)
        
        ttk.Button(act_frame, text="📈 Generate Report Preview", width=35, 
                   command=lambda: self.start_bg_task(self.generate_report_preview)).grid(row=0, column=0, columnspan=2, pady=5)
        
        ttk.Button(act_frame, text="📊 Excel Report", width=20, 
                   command=lambda: self.start_bg_task(self.generate_report, 'xlsx')).grid(row=1, column=0, padx=5)
        
        ttk.Button(act_frame, text="📊 CSV Report", width=20, 
                   command=lambda: self.start_bg_task(self.generate_report, 'csv')).grid(row=1, column=1, padx=5)

    def setup_preview_section(self):
        # Header Info
        header_row = ttk.Frame(self.main_frame)
        header_row.pack(fill="x", pady=(10, 0))
        
        self.preview_header = ttk.Label(header_row, text="Preview Panel", font=("Segoe UI", 10, "bold"))
        self.preview_header.pack(side="left")
        
        # Global Search
        search_frame = ttk.Frame(self.main_frame)
        search_frame.pack(fill="x", pady=5)
        
        ttk.Label(search_frame, text="🔍 Global Search:").pack(side="left", padx=5)
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=40)
        self.search_entry.pack(side="left", padx=5)
        self.search_entry.bind("<KeyRelease>", lambda e: self.debounce_filter())
        
        ttk.Button(search_frame, text="Clear All Filters", command=self.clear_all_filters).pack(side="left", padx=10)
        
        self.search_count_var = tk.StringVar(value="0 rows")
        ttk.Label(search_frame, textvariable=self.search_count_var, font=("Segoe UI", 9, "italic")).pack(side="right", padx=10)

        # --- Treeview with Aligned Scrolling Filters ---
        tree_container = ttk.Frame(self.main_frame)
        tree_container.pack(fill="both", expand=True)
        
        # 1. Filter Row (Canvas + Frame)
        self.filter_canvas = tk.Canvas(tree_container, height=34, highlightthickness=0, bg="#E1E1E1")
        self.filter_canvas.grid(row=0, column=0, sticky="ew")
        
        self.filter_frame = tk.Frame(self.filter_canvas, bg="#E1E1E1")
        self.filter_window = self.filter_canvas.create_window((0, 0), window=self.filter_frame, anchor="nw")

        # 2. Treeview
        self.tree = ttk.Treeview(tree_container, show="headings", height=18)
        self.tree.grid(row=1, column=0, sticky="nsew")
        
        # Bindings for dynamic filter alignment
        self.tree.bind("<Configure>", lambda e: self.root.after(1, self.sync_filter_widths))
        self.root.bind("<Motion>", lambda e: self.sync_filter_widths()) # Fallback for column resizing
        
        # 3. Scrollbars
        self.vsb = ttk.Scrollbar(tree_container, orient="vertical", command=self.tree.yview)
        self.vsb.grid(row=1, column=1, sticky="ns")
        
        self.hsb = ttk.Scrollbar(tree_container, orient="horizontal", command=self.sync_h_scroll)
        self.hsb.grid(row=2, column=0, sticky="ew")

        self.tree.configure(yscrollcommand=self.vsb.set, xscrollcommand=self.hsb.set)
        
        tree_container.columnconfigure(0, weight=1)
        tree_container.rowconfigure(1, weight=1)

        # tags for styling
        self.tree.tag_configure('negative', background='#FFCDD2') # Reddish
        self.tree.tag_configure('totals', font=("Segoe UI", 9, "bold"), background='#EEEEEE')

        # Context Menu
        self.menu = tk.Menu(self.root, tearoff=0)
        self.menu.add_command(label="📋 Copy Value", command=self.copy_cell)
        self.menu.add_command(label="📋 Copy Row", command=self.copy_row)
        self.menu.add_separator()
        self.menu.add_command(label="📋 Copy All Preview", command=self.copy_table)
        self.tree.bind("<Button-3>", self.show_context_menu)

    def setup_footer(self):
        footer = ttk.Frame(self.main_frame)
        footer.pack(fill="x", pady=10)
        
        self.progress_var = tk.DoubleVar()
        self.progress = ttk.Progressbar(footer, variable=self.progress_var, maximum=100)
        self.progress.pack(side="left", fill="x", expand=True, padx=10)
        
        self.status_label = ttk.Label(footer, text="System Ready", foreground="#2B5797", font=("Segoe UI", 9, "bold"))
        self.status_label.pack(side="left", padx=10)
        
        self.open_folder_btn = ttk.Button(footer, text="📁 Open Save Location", state="disabled", command=self.open_last_folder)
        self.open_folder_btn.pack(side="right", padx=10)

    # ────────────────────────────────────────────────
    # LOGIC & THREADING
    # ────────────────────────────────────────────────
    def start_bg_task(self, func, *args):
        """Thread wrapper to keep UI responsive."""
        self.save_config()
        self.progress_var.set(15)
        self.status_label.config(text="Processing Request...", foreground="orange")
        self.open_folder_btn.config(state="disabled")
        threading.Thread(target=func, args=args, daemon=True).start()

    def sync_h_scroll(self, *args):
        """Scroll both Treeview and Filter row at the same time."""
        self.tree.xview(*args)
        self.filter_canvas.xview(*args)

    def set_quick_date(self, days):
        today = datetime.now()
        self.end_date_entry.delete(0, tk.END)
        self.end_date_entry.insert(0, today.strftime("%Y-%m-%d"))
        
        self.start_date_entry.delete(0, tk.END)
        if days == -1: # Current Month Start
            start = today.replace(day=1)
        else:
            start = today - timedelta(days=days)
        self.start_date_entry.insert(0, start.strftime("%Y-%m-%d"))

    def validate_dates(self): 
        try: 
            s = datetime.strptime(self.start_date_entry.get(), "%Y-%m-%d") 
            e = datetime.strptime(self.end_date_entry.get(), "%Y-%m-%d") 
            if s > e: 
                messagebox.showwarning("Invalid Range", "Start date cannot be after end date.") 
                return False 
            return True 
        except: 
            messagebox.showerror("Invalid Date", "Please use YYYY-MM-DD format for both dates.") 
            return False

    def build_report_sql(self, report_type, start_date, end_date, off_type, selected_bus):
        """Build parameterized SQL query."""
        params = {
            "start": f"{start_date} 00:00:00",
            "end": f"{end_date} 23:59:59"
        }
        
        filters = " WHERE main.date_of_payment BETWEEN :start AND :end"
        
        if off_type != "All":
            filters += " AND c.officer_type = :off_type"
            params["off_type"] = off_type
            
        if "All" not in selected_bus and selected_bus:
            # Secure dynamic IN clause
            in_markers = []
            for i, bu in enumerate(selected_bus):
                marker = f"bu_{i}"
                in_markers.append(f":{marker}")
                params[marker] = bu
            filters += f" AND c.business_unit IN ({', '.join(in_markers)})"
            
        if report_type == "listing":
            sql = f"""
                SELECT 
                    main.account_number, c.account_name, c.account_address, 
                    main.date_of_payment, c.account_officer, 
                    c.business_unit, c.undertaking,
                    COALESCE(afs.total_payments, 0) as total_payments, 
                    COALESCE(afs.total_discounts, 0) as total_discounts, 
                    COALESCE(afs.total_adjustments, 0) as total_adjustments, 
                    COALESCE(afs.outstanding_balance, 0) as outstanding_balance,
                    COALESCE(afs.payment_plan, 'No') as payment_plan
                FROM all_payments main
                LEFT JOIN customers c ON main.account_number = c.account_number
                LEFT JOIN account_financial_summary afs ON afs.account_number = main.account_number
                {filters}
                ORDER BY main.date_of_payment DESC
            """
        else:
            # Summary report requires some literal dates for sub-aggregations, 
            # but we use placeholders where possible.
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            today_str = end_dt.strftime('%Y-%m-%d')
            yest_str = (end_dt - timedelta(days=1)).strftime('%Y-%m-%d')
            week_str = (end_dt - timedelta(days=end_dt.weekday())).strftime('%Y-%m-%d')
            mon_str = end_dt.strftime('%Y-%m-01')

            sql = f"""
                SELECT 
                    c.business_unit,
                    c.account_officer,
                    SUM(CASE WHEN DATE(main.date_of_payment) = :today THEN main.amount_paid ELSE 0 END) as Today_Recovery,
                    SUM(CASE WHEN DATE(main.date_of_payment) = :yesterday THEN main.amount_paid ELSE 0 END) as Yesterday_Recovery,
                    SUM(CASE WHEN DATE(main.date_of_payment) >= :week_start THEN main.amount_paid ELSE 0 END) as This_Week_Recovery,
                    SUM(CASE WHEN DATE(main.date_of_payment) >= :month_start THEN main.amount_paid ELSE 0 END) as This_Month_Recovery
                FROM all_payments main
                LEFT JOIN customers c ON main.account_number = c.account_number
                {filters}
                GROUP BY c.business_unit, c.account_officer
                ORDER BY This_Month_Recovery DESC
            """
            params.update({
                "today": today_str,
                "yesterday": yest_str,
                "week_start": week_str,
                "month_start": mon_str
            })
        return sql, params

    # ────────────────────────────────────────────────
    # CORE ACTIONS
    # ────────────────────────────────────────────────
    def export_table_preview(self):
        tbl = self.table_var.get()
        if not tbl: return
        self.fetch_preview(f"SELECT * FROM {tbl}", {})

    def generate_report_preview(self):
        if not self.validate_dates(): return
        try:
            sql, params = self.build_report_sql(
                self.report_type_var.get(),
                self.start_date_entry.get(),
                self.end_date_entry.get(),
                self.officer_type_var.get(),
                [self.bu_listbox.get(i) for i in self.bu_listbox.curselection()]
            )
            self.fetch_preview(sql, params)
        except Exception as e:
            self.root.after(0, lambda e=e: messagebox.showerror("Error", f"Failed to build report: {e}"))

    def fetch_preview(self, sql, params):
        try:
            df = pd.read_sql(text(sql), self.engine, params=params)
            self.root.after(0, lambda df=df: self.render_preview(df))
        except Exception as e:
            self.root.after(0, lambda e=e: self.handle_error("Query Error", e))

    def sync_filter_widths(self, *args):
        """Update filter entry widths to match Treeview column widths."""
        if not hasattr(self, 'column_filters') or not self.column_filters: return
        
        cols = self.tree["columns"]
        entries = self.filter_frame.winfo_children()
        
        if len(cols) != len(entries): return
        
        x_pos = 0
        for i, col in enumerate(cols):
            actual_w = self.tree.column(col, "width")
            ent = entries[i]
            # Use place to force exact pixel width and height matching the header
            ent.place_forget() 
            ent.place(x=x_pos, y=2, width=actual_w, height=30)
            x_pos += actual_w
        
        # Sync the scrollregion of the filter canvas
        self.filter_canvas.config(scrollregion=(0, 0, x_pos, 34))
        # Sync the window width
        self.filter_canvas.itemconfig(self.filter_window, width=x_pos, height=34)

    def autosize_columns(self, df):
        """Estimate column widths based on content."""
        if df.empty: return
        
        # Sample some rows for faster processing if df is huge
        sample = df.head(1000)
        
        for col in df.columns:
            # Header width
            header_w = len(str(col)) * 10 + 30
            # Max content width
            max_content_w = sample[col].astype(str).str.len().max() * 8 + 20
            
            final_w = min(max(header_w, max_content_w), 400) # Reasonable max width
            self.tree.column(col, width=int(final_w), stretch=False)

    def debounce_filter(self):
        """Debounce filtering to improve performance with large datasets."""
        if self._filter_timer:
            self.root.after_cancel(self._filter_timer)
        self._filter_timer = self.root.after(400, self.filter_preview)

    def clear_all_filters(self):
        """Clear global search and all column filters."""
        self.search_var.set("")
        for col, var in self.column_filters.items():
            var.set("")
            # Reset placeholder visual if entry exists
            for child in self.filter_frame.winfo_children():
                if isinstance(child, ttk.Entry) and child.cget("textvariable") == str(var):
                    if not child.get():
                        child.insert(0, col)
                        child.config(foreground="grey")
        self.filter_preview()

    def on_tab_switch(self):
        """Clear filters and preview when switching tabs."""
        # Check if we have data before clearing
        if not self.current_df.empty:
            self.clear_all_filters()
            # Clear dataframes to free memory
            self.current_df = pd.DataFrame()
            self.filtered_df = pd.DataFrame()
            # Clear Treeview
            for i in self.tree.get_children(): self.tree.delete(i)
            # Clear column filters UI
            for w in self.filter_frame.winfo_children(): w.destroy()
            self.column_filters = {}
            self.preview_header.config(text="Preview Panel")
            self.search_count_var.set("0 rows")

    def render_preview(self, df):
        self.current_df = df
        self.filtered_df = df.copy()
        count = len(df)
        self.preview_header.config(text=f"Preview (First 1,000 Rows) — Total Records: {count:,}")
        
        # 1. Update Columns first (REQUIRED before configuring headings/autosizing)
        cols = list(df.columns)
        self.tree["columns"] = cols
        
        # 2. Autosize based on content
        self.autosize_columns(df)
        
        # 3. Filter/Render Data (This will create filter boxes if needed)
        self.filter_preview() 
        
        self.progress_var.set(100)
        self.status_label.config(text="Data Loaded Successfully", foreground="green")
        
        # 4. Sync Filter Widths (Ensure they align with the new column sizes)
        # Give Tkinter a moment to render the treeview columns properly
        self.root.after(200, self.sync_filter_widths)

    def filter_preview(self):
        if self.current_df.empty: return
        
        df = self.current_df.copy()
        
        # 1. Global Filter
        q = self.search_var.get().lower().strip()
        if q:
            mask = df.astype(str).apply(lambda x: x.str.lower().str.contains(q, na=False)).any(axis=1)
            df = df[mask]
        
        # 2. Column-specific Filters
        for col, var in self.column_filters.items():
            val = var.get().lower().strip()
            if val and val != col.lower():
                df = df[df[col].astype(str).str.lower().str.contains(val, na=False)]
        
        self.filtered_df = df
        self.update_treeview_data(df)
        self.search_count_var.set(f"{len(df):,} of {len(self.current_df):,} rows")

    def update_treeview_data(self, df):
        # Clear existing rows
        for i in self.tree.get_children(): self.tree.delete(i)
        
        cols = list(df.columns)
        
        # Check if we need to rebuild the filter header (columns changed)
        current_filter_cols = list(self.column_filters.keys())
        if cols != current_filter_cols:
            for w in self.filter_frame.winfo_children(): w.destroy()
            self.column_filters = {}
            for col in cols:
                var = tk.StringVar()
                self.column_filters[col] = var
                ent = ttk.Entry(self.filter_frame, textvariable=var, font=("Segoe UI", 8))
                
                # Placeholder logic
                ent.insert(0, col)
                ent.config(foreground="grey")
                ent.bind("<FocusIn>", lambda e, en=ent, c=col: self.filter_focus(en, c, True))
                ent.bind("<FocusOut>", lambda e, en=ent, c=col: self.filter_focus(en, c, False))
                ent.bind("<KeyRelease>", lambda e: self.debounce_filter())
                
                # Initial place - will be corrected by sync_filter_widths
                ent.place(x=0, y=0, width=10, height=30) 
            
            self.filter_frame.update_idletasks()
            self.sync_filter_widths() # Force alignment after creation

        # Setup Headers & Formatting
        money_cols = ['total_payments', 'total_discounts', 'total_adjustments', 'outstanding_balance', 
                      'Today_Recovery', 'Yesterday_Recovery', 'This_Week_Recovery', 'This_Month_Recovery']
        
        for col in cols:
            self.tree.heading(col, text=col.upper(), command=lambda c=col: self.sort_by(c))
            self.tree.column(col, anchor="center", stretch=False) # Width already set in autosize_columns

        # Insert Data (Limit preview to 1,000)
        view_df = df.head(1000)
        for idx, row in view_df.iterrows():
            display_vals = []
            for col in cols:
                val = row[col]
                if col in money_cols and pd.notna(val):
                    display_vals.append(f"₦{float(val):,.2f}")
                else:
                    display_vals.append(str(val) if pd.notna(val) else "")
            
            tag = ()
            if 'outstanding_balance' in cols and row['outstanding_balance'] < 0:
                tag = ('negative',)
            self.tree.insert("", "end", values=display_vals, tags=tag)

        # Totals Row
        if not df.empty:
            sums = []
            for col in cols:
                if col in money_cols:
                    sums.append(f"₦{df[col].sum():,.2f}")
                elif col == cols[0]:
                    sums.append("GRAND TOTAL")
                else:
                    sums.append("")
            self.tree.insert("", "end", values=sums, tags=('totals',))

    def filter_focus(self, ent, col, is_in):
        if is_in and ent.get() == col:
            ent.delete(0, tk.END)
            ent.config(foreground="black")
        elif not is_in and not ent.get():
            ent.insert(0, col)
            ent.config(foreground="grey")

    def sort_by(self, col):
        """Toggle sort for the current dataframe."""
        if self.current_df.empty: return
        
        ascending = True
        if hasattr(self, '_last_sort') and self._last_sort == col:
            ascending = not getattr(self, '_last_asc', True)
        
        # Sort current_df
        self.current_df = self.current_df.sort_values(by=col, ascending=ascending)
        self._last_sort = col
        self._last_asc = ascending
        
        # Re-apply filters to updated current_df
        self.filter_preview()

    # ────────────────────────────────────────────────
    # EXPORTING
    # ────────────────────────────────────────────────
    def export_table_data(self, fmt):
        tbl = self.table_var.get()
        if not tbl: return
        stamp = datetime.now().strftime('%Y%m%d')
        self.run_export(f"SELECT * FROM {tbl}", {}, f"{tbl}_Export_{stamp}", fmt)

    def generate_report(self, fmt):
        if not self.validate_dates(): return
        try:
            sql, params = self.build_report_sql(
                self.report_type_var.get(),
                self.start_date_entry.get(),
                self.end_date_entry.get(),
                self.officer_type_var.get(),
                [self.bu_listbox.get(i) for i in self.bu_listbox.curselection()]
            )
            stamp = datetime.now().strftime('%Y%m%d')
            off_type = self.officer_type_var.get().replace(' ', '_')
            report_name = self.report_type_var.get().replace(' ', '_')
            fname = f"{off_type}_{report_name}_{self.start_date_entry.get()}_to_{self.end_date_entry.get()}_{stamp}"
            self.run_export(sql, params, fname, fmt)
        except Exception as e:
            self.root.after(0, lambda e=e: messagebox.showerror("Export Error", str(e)))

    def run_export(self, sql, params, filename, fmt):
        try:
            # 1. Fetch FULL dataset matching criteria (not just preview)
            self.root.after(0, lambda: self.status_label.config(text="Fetching complete dataset..."))
            full_df = pd.read_sql(text(sql), self.engine, params=params)
            
            # 2. Apply the SAME local filters as seen in the UI
            q = self.search_var.get().lower().strip()
            if q:
                full_df = full_df[full_df.astype(str).apply(lambda x: x.str.lower().str.contains(q, na=False)).any(axis=1)]
            
            for col, var in self.column_filters.items():
                val = var.get().lower().strip()
                if val and val != col.lower():
                    full_df = full_df[full_df[col].astype(str).str.lower().str.contains(val, na=False)]

            if full_df.empty:
                self.root.after(0, lambda: messagebox.showwarning("Empty Result", "The filtered data contains no rows to export."))
                return

            # 3. File Save Dialog
            ext = f".{fmt}"
            fpath = filedialog.asksaveasfilename(initialfile=filename + ext, defaultextension=ext,
                                                 filetypes=[("Excel" if fmt=='xlsx' else "CSV", f"*{ext}")])
            if not fpath:
                self.root.after(0, lambda: self.status_label.config(text="Export Cancelled", foreground="black"))
                return

            # 4. Write to disk
            self.root.after(0, lambda full_df=full_df: self.status_label.config(text=f"Saving {len(full_df):,} rows..."))
            if fmt == 'csv':
                full_df.to_csv(fpath, index=False)
            else:
                full_df.to_excel(fpath, index=False)
            
            self.last_export_path = fpath
            self.root.after(0, self.export_done)
        except Exception as e:
            self.root.after(0, lambda e=e: self.handle_error("Export Failed", e))

    def export_done(self):
        self.progress_var.set(100)
        self.status_label.config(text="Export Completed!", foreground="green")
        self.open_folder_btn.config(state="normal")
        messagebox.showinfo("Success", f"File successfully saved to:\n{self.last_export_path}")

    def open_last_folder(self):
        if self.last_export_path:
            os.startfile(os.path.dirname(self.last_export_path))

    # ────────────────────────────────────────────────
    # DATA METADATA & PERSISTENCE
    # ────────────────────────────────────────────────
    def save_config(self):
        try:
            config = {
                "tab_index": self.notebook.index("current"),
                "date_start": self.start_date_entry.get(),
                "date_end": self.end_date_entry.get(),
                "report_mode": self.report_type_var.get(),
                "off_type": self.officer_type_var.get(),
                "selected_bus_indices": [i for i in self.bu_listbox.curselection()]
            }
            with open(CONFIG_FILE, 'w') as f: json.dump(config, f)
        except: pass

    def load_config(self):
        if not os.path.exists(CONFIG_FILE): return
        try:
            with open(CONFIG_FILE, 'r') as f:
                c = json.load(f)
                self.notebook.select(c.get("tab_index", 0))
                self.start_date_entry.insert(0, c.get("date_start", ""))
                self.end_date_entry.insert(0, c.get("date_end", ""))
                self.report_type_var.set(c.get("report_mode", "listing"))
                self.officer_type_var.set(c.get("off_type", "All"))
                for idx in c.get("selected_bus_indices", []):
                    self.bu_listbox.select_set(idx)
        except: pass

    def refresh_metadata(self):
        """Load table list and business units."""
        try:
            with self.engine.connect() as conn:
                # Tables
                res = conn.execute(text("SHOW TABLES"))
                tables = [r[0] for r in res.fetchall() if not r[0].startswith("staging_")]
                self.table_dropdown['values'] = tables
                
                # BUs
                res_bu = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE business_unit IS NOT NULL ORDER BY business_unit"))
                self.bu_listbox.delete(0, tk.END)
                self.bu_listbox.insert(tk.END, "All")
                for r in res_bu.fetchall():
                    self.bu_listbox.insert(tk.END, r[0])
        except: pass

    def handle_error(self, title, err):
        self.status_label.config(text="Critical Error", foreground="red")
        messagebox.showerror(title, f"An unexpected error occurred:\n{err}")

    # ────────────────────────────────────────────────
    # CONTEXT MENU & CLIPBOARD
    # ────────────────────────────────────────────────
    def show_context_menu(self, event):
        item = self.tree.identify_row(event.y)
        if item:
            self.tree.selection_set(item)
            # Store the column that was clicked for copy_cell
            self._last_clicked_col = self.tree.identify_column(event.x)
            self.menu.post(event.x_root, event.y_root)

    def copy_cell(self):
        sel = self.tree.selection()
        if not sel or not hasattr(self, '_last_clicked_col'): return
        try:
            col_idx = int(self._last_clicked_col.replace("#", "")) - 1
            val = self.tree.item(sel[0])['values'][col_idx]
            # Clean formatting if it's money (remove ₦ and commas)
            clean_val = str(val).replace("₦", "").replace(",", "")
            self.root.clipboard_clear()
            self.root.clipboard_append(clean_val)
        except: pass

    def copy_row(self):
        sel = self.tree.selection()
        if not sel: return
        vals = self.tree.item(sel[0])['values']
        self.root.clipboard_clear()
        self.root.clipboard_append("\t".join(map(str, vals)))

    def copy_table(self):
        lines = ["\t".join(self.tree["columns"])]
        for item in self.tree.get_children():
            lines.append("\t".join(map(str, self.tree.item(item)['values'])))
        self.root.clipboard_clear()
        self.root.clipboard_append("\n".join(lines))

if __name__ == "__main__":
    app_root = tk.Tk()
    ExportTool(app_root)
    app_root.mainloop()
