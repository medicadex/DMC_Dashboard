import sys
import threading
import time
import os
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory, session, make_response, Response
from werkzeug.utils import secure_filename
from db_utils import get_db_engine
from datetime import datetime, timedelta
import logging
from sqlalchemy import text
import pandas as pd
from functools import wraps

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from services.account_service import AccountService
from services.validation_service import ValidationService
from repositories.staff_repo import StaffRepository
from services.job_form_service import JobFormService
from services.auth_service import AuthService
from services.upload_service import UploadService
from services.reporting_service import ReportingService
from services.admin_report_service import AdminReportService
from services.export_service import ExportService
from utils.security import SessionManager
from db_utils import get_db_engine

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def generate_descriptive_filename(base_name, filters):
    """
    Generates a descriptive filename based on applied filters.
    """
    parts = [base_name]
    
    # Handle Officer Type
    otype = filters.get('otype') or filters.get('otypes')
    if otype:
        if isinstance(otype, list):
            if len(otype) == 1: parts.append(otype[0])
            elif len(otype) > 1: parts.append("MixedTypes")
        elif otype != 'Both':
            parts.append(otype)
            
    # Handle Period/Date Range
    if filters.get('period'):
        parts.append(filters['period'].capitalize())
    
    if filters.get('start') and filters.get('end'):
        parts.append(f"{filters['start']}_to_{filters['end']}")
    elif filters.get('year'):
        parts.append(filters['year'])
        if filters.get('quarter') and filters['quarter'] != 'Full':
            parts.append(filters['quarter'])
            
    # Handle Business Unit
    bus = filters.get('bu') or filters.get('bus')
    if bus:
        if isinstance(bus, list):
            if len(bus) == 1: parts.append(bus[0])
            elif len(bus) > 1: parts.append(f"{len(bus)}BUs")
        else:
            parts.append(bus)

    # Handle Form Type (Job Form specific)
    if filters.get('ftype'):
        parts.append(filters['ftype'])

    parts.append(datetime.now().strftime('%Y%m%d_%H%M%S'))
    
    # Sanitize and join
    filename = "_".join([str(p) for p in parts if p])
    # Remove chars that might cause issues in URLs or Filesystems
    for char in [' ', ',', '/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        filename = filename.replace(char, "_")
    
    return f"{filename}.xlsx"

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
engine = get_db_engine()
staff_repo = StaffRepository(engine)
validation_service = ValidationService(engine)
account_service = AccountService(engine, staff_repo, validation_service)
job_form_service = JobFormService(engine)
session_manager = SessionManager(timeout_minutes=15)
auth_service = AuthService(staff_repo, session_manager)
upload_service = UploadService(engine, staff_repo)
reporting_service = ReportingService(engine, staff_repo)
admin_report_service = AdminReportService(engine, staff_repo)
export_service = ExportService(engine)

# Cache busting version
APP_VERSION = "1.1.7"
ENCRYPTION_SECRET = os.getenv("ENCRYPTION_SECRET")

@app.context_processor
def inject_version():
    return dict(version=APP_VERSION)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login', next=request.url))
        
        # Inactivity timeout check
        if not session_manager.is_session_valid():
            session.clear()
            return redirect(url_for('login', next=request.url))
            
        session_manager.update_activity()
        return f(*args, **kwargs)
    return decorated_function

@app.after_request
def add_header(response):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 0 seconds.
    """
    # Force no-cache for Service Worker and Manifest to allow updates
    if request.path.endswith('service-worker.js') or request.path.endswith('manifest.json'):
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
        return response

    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response

# --- ROUTES ---

@app.route('/')
@login_required
def index():
    return render_template('home.html', active_page='home')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        try:
            user_data = auth_service.login(username, password)
            if user_data:
                session['user'] = user_data
                return jsonify({'success': True, 'user': user_data})
            return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 401
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', active_page='dashboard')

# Rest of the routes restored here...
@app.route('/job-form')
@login_required
def job_form():
    return render_template('job_form.html', active_page='job_form')

@app.route('/payment-listing')
@login_required
def payment_listing():
    return render_template('payments.html', active_page='payments')

@app.route('/customer-listing')
@login_required
def customer_listing():
    return render_template('customers.html', active_page='customers')

@app.route('/performance-rank')
@login_required
def performance_rank():
    return render_template('performance.html', active_page='performance')

@app.route('/performance/full-report')
@login_required
def performance_full_report():
    return render_template('performance_full.html', active_page='performance')

@app.route('/report-uploader', methods=['GET', 'POST'])
@login_required
def report_uploader():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({"error": "No file part"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400
            
        if file:
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            try:
                # Defaulting to 'collections' for general performance reports
                result = upload_service.process_table('collections', filepath, session['user']['username'])
                return jsonify({
                    "success": True, 
                    "message": f"Successfully processed {result.get('total', 0)} rows. {result.get('new', 0)} new records added.",
                    "details": result
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
                
    return render_template('upload.html', active_page='upload')
    
@app.route('/admin/upload-tables', methods=['GET', 'POST'])
@login_required
def admin_upload_tables():
    """Admin-only route for bulk uploading data to multiple RDS tables simultaneously."""
    if session['user'].get('role') != 'Admin':
        return redirect(url_for('index'))

    # Hardcoded set of allowed tables — only these can be uploaded via the admin panel
    ALLOWED_TABLES = ['collections', 'validation', 'disconnections', 'adjustments', 'discounts', 'customers']

    if request.method == 'POST':
        results = []
        errors  = []

        for table_name in ALLOWED_TABLES:
            file_key = f"file_{table_name}"
            file = request.files.get(file_key)

            # Skip tables where no file was attached
            if not file or file.filename == '':
                continue

            filename = secure_filename(file.filename)
            if not filename.lower().endswith(('.xlsx', '.xls')):
                errors.append({"table": table_name, "error": "Invalid file type. Only .xlsx/.xls allowed."})
                continue

            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            try:
                result = upload_service.process_table(
                    table_name, filepath, session['user']['username']
                )
                results.append({
                    "table":      table_name,
                    "total":      result.get('total', 0),
                    "new":        result.get('new', 0),
                    "duplicates": result.get('duplicates', 0),
                    "officer_changes": result.get('officer_changes', {}),
                    "status":     "success"
                })
            except Exception as e:
                logger.error(f"Admin upload failed for '{table_name}': {e}")
                errors.append({"table": table_name, "error": str(e)})
            finally:
                # Clean up the uploaded file after processing
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                except Exception:
                    pass

        return jsonify({"results": results, "errors": errors})

    return render_template('admin_upload.html', active_page='admin_upload')


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'ie_logo.png', mimetype='image/png')

@app.route('/manifest.json')
def serve_manifest():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'manifest.json')

@app.route('/service-worker.js')
def service_worker():
    # Return a script that simply unregisters itself
    content = "self.registration.unregister().then(() => self.clients.matchAll().then(c => c.forEach(cl => cl.navigate(cl.url))));"
    resp = Response(content, mimetype='application/javascript')
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    resp.headers['Clear-Site-Data'] = '"cache", "storage"'
    return resp

# --- API ENDPOINTS ---

@app.route('/api/dashboard-stats', methods=['GET'])
@login_required
def get_dashboard_stats():
    try:
        from sqlalchemy import text
        from datetime import datetime, timedelta
        import math
        
        now = datetime.now()
        today_start = now.strftime('%Y-%m-%d 00:00:00')
        month_start = now.strftime('%Y-%m-01 00:00:00')
        
        data = {}
        with engine.connect() as conn:
            # Get all active Business Units first to ensure they all show up
            res_bus = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE business_unit IS NOT NULL AND business_unit != '' ORDER BY business_unit ASC")).fetchall()
            all_bus = [r[0] for r in res_bus]

            # 1. Monthly BU (Full list)
            sql_monthly = """
                SELECT c.business_unit, SUM(p.amount_paid) as total
                FROM (
                    SELECT account_number, amount_paid, date_of_payment FROM collections
                    UNION ALL
                    SELECT account_number, amount_paid, date_of_payment FROM other_payments
                ) p
                JOIN customers c ON p.account_number = c.account_number
                WHERE p.date_of_payment >= :mstart
                GROUP BY c.business_unit
            """
            res_m = conn.execute(text(sql_monthly), {"mstart": month_start}).fetchall()
            m_map = {r[0]: float(r[1]) for r in res_m if r[0]}
            data['monthly_bu'] = [{"bu": bu, "total": m_map.get(bu, 0.0)} for bu in all_bus]
            
            # 2. Weekly Breakdown for current month
            sql_weeks = """
                SELECT 
                    c.business_unit,
                    FLOOR((DAY(p.date_of_payment) - 1) / 7) + 1 as week_num,
                    SUM(p.amount_paid) as total
                FROM (
                    SELECT account_number, amount_paid, date_of_payment FROM collections
                    UNION ALL
                    SELECT account_number, amount_paid, date_of_payment FROM other_payments
                ) p
                JOIN customers c ON p.account_number = c.account_number
                WHERE p.date_of_payment >= :mstart
                GROUP BY c.business_unit, week_num
                ORDER BY week_num ASC
            """
            res_w = conn.execute(text(sql_weeks), {"mstart": month_start}).fetchall()
            
            # Process weeks
            weeks_present = sorted(list(set(r[1] for r in res_w)))
            weekly_data = []
            for w in weeks_present:
                bu_totals = {r[0]: float(r[2]) for r in res_w if r[1] == w and r[0]}
                weekly_data.append({
                    "week_label": f"Week {int(w)}",
                    "stats": [{"bu": bu, "total": bu_totals.get(bu, 0.0)} for bu in all_bus]
                })
            data['weekly_breakdown'] = weekly_data
            
            # 3. Daily BU (Full list)
            sql_daily = """
                SELECT c.business_unit, SUM(p.amount_paid) as total
                FROM (
                    SELECT account_number, amount_paid, date_of_payment FROM collections
                    UNION ALL
                    SELECT account_number, amount_paid, date_of_payment FROM other_payments
                ) p
                JOIN customers c ON p.account_number = c.account_number
                WHERE p.date_of_payment >= :dstart
                GROUP BY c.business_unit
            """
            res_d = conn.execute(text(sql_daily), {"dstart": today_start}).fetchall()
            d_map = {r[0]: float(r[1]) for r in res_d if r[0]}
            data['daily_bu'] = [{"bu": bu, "total": d_map.get(bu, 0.0)} for bu in all_bus]
            
            # 4. Top/Bottom Officers Monthly (Cleaned of 'Unknown')
            sql_dmo = """
                SELECT c.account_officer as officer, 
                       c.business_unit as bu,
                       SUM(p.amount_paid) as total
                FROM (
                    SELECT account_number, amount_paid, date_of_payment FROM collections
                    UNION ALL
                    SELECT account_number, amount_paid, date_of_payment FROM other_payments
                ) p
                JOIN customers c ON p.account_number = c.account_number
                WHERE p.date_of_payment >= :mstart
                  AND c.business_unit IS NOT NULL AND c.business_unit != ''
                  AND c.account_officer IS NOT NULL AND c.account_officer != ''
                GROUP BY officer, bu HAVING total > 0 ORDER BY total DESC
            """
            res_dmo = conn.execute(text(sql_dmo), {"mstart": month_start}).fetchall()
            dmo_list = [{"dmo": r[0], "bu": r[1], "total": float(r[2])} for r in res_dmo]
            
            data['top_dmo'] = dmo_list[:5]
            data['bottom_dmo'] = dmo_list[-5:][::-1] if len(dmo_list) >= 5 else dmo_list[::-1]
            
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        import logging
        logging.error(f"Dashboard stats error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/autocomplete')
@login_required
def api_autocomplete():
    query = request.args.get('q', '').strip()
    if len(query) < 3:
        return jsonify([])

    search_term = f"%{query}%"
    sql = text("""
        SELECT account_number, account_name, account_address
        FROM customers
        WHERE account_number LIKE :q OR account_name LIKE :q OR account_address LIKE :q
        LIMIT 30
    """)
    
    with engine.connect() as conn:
        results = conn.execute(sql, {"q": search_term}).fetchall()
        
    return jsonify([dict(r._mapping) for r in results])

@app.route('/api/account_dashboard/<account_number>')
@login_required
def api_account_dashboard(account_number):
    try:
        data = account_service.get_account_financials(account_number, session['user']['username'], session['user']['role'], force_online=True)
        if not data:
            return jsonify({"error": "Account not found"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/account/update-start-date', methods=['POST'])
@login_required
def api_update_start_date():
    if session['user'].get('role') != 'Admin':
        return jsonify({"success": False, "message": "Unauthorized. Admins only."}), 403
    
    data = request.get_json()
    account_number = data.get('account_number')
    new_date = data.get('start_date')
    
    if not account_number or not new_date:
        return jsonify({"success": False, "message": "Account number and start date are required"}), 400
        
    try:
        success, message = account_service.update_officer_start_date(account_number, new_date)
        if success:
            return jsonify({"success": True, "message": message})
        return jsonify({"success": False, "message": message}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/account/update-officer-dates-batch', methods=['POST'])
@login_required
def api_update_officer_dates_batch():
    if session['user'].get('role') != 'Admin':
        return jsonify({"success": False, "message": "Unauthorized. Admins only."}), 403
        
    data = request.get_json()
    updates = data.get('updates') # list of dicts: [{'account_number': '...', 'start_date': '...'}]
    
    if not updates or not isinstance(updates, list):
        return jsonify({"success": False, "message": "List of updates is required"}), 400
        
    try:
        success, message = account_service.update_officer_start_dates_batch(updates)
        if success:
            return jsonify({"success": True, "message": message})
        return jsonify({"success": False, "message": message}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/job-form/initial-data')
@login_required
def job_form_initial_data():
    try:
        bus = job_form_service.get_distinct_values("customers", "business_unit")
        return jsonify({"business_units": bus})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/job-form/officers')
@login_required
def job_form_officers():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    if not bus or not otypes:
        return jsonify([])
    try:
        names = job_form_service.get_officer_names(bus, otypes)
        return jsonify(names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/job-form/feeders')
@login_required
def job_form_feeders():
    bus = request.args.getlist('bu')
    names = request.args.getlist('name')
    if not bus or not names:
        return jsonify([])
    try:
        feeders = job_form_service.get_feeders(bus, names)
        return jsonify(feeders)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/job-form/dts')
@login_required
def job_form_dts():
    bus = request.args.getlist('bu')
    feeders = request.args.getlist('feeder')
    if not bus or not feeders:
        return jsonify([])
    try:
        dts = job_form_service.get_dt_names(bus, feeders)
        return jsonify(dts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/job-form/preview')
@login_required
def job_form_preview():
    filters = {
        'bus': request.args.getlist('bu'),
        'otypes': request.args.getlist('type'),
        'onames': request.args.getlist('name'),
        'feeders': request.args.getlist('feeder'),
        'dts': request.args.getlist('dt'),
        'ftype': request.args.get('form_type', 'Full')
    }
    # Default columns for preview
    columns = ["account_number", "account_name", "account_address", "closing_balance", "pos_other_payments", "adjustment", "discount", "outstanding_balance", "account_officer"]
    
    page = int(request.args.get('page', 1))
    per_page_arg = request.args.get('per_page', '50')
    
    try:
        df = job_form_service.get_job_form_data(filters, columns)
        if df.empty:
            return jsonify({
                "data": [],
                "total_rows": 0,
                "pages": 0,
                "current_page": 1,
                "per_page": 50,
                "totals": {}
            })
            
        # Calculate Grand Totals over the entire filtered dataset
        numeric_cols = ["closing_balance", "pos_other_payments", "adjustment", "discount", "outstanding_balance"]
        totals = {}
        for col in numeric_cols:
            if col in df.columns:
                totals[col] = float(df[col].fillna(0).sum())
                
        total_rows = int(df.shape[0])
        
        if per_page_arg.lower() == 'all':
            per_page = total_rows
            pages = 1
            df_sliced = df
        else:
            per_page = int(per_page_arg)
            import math
            pages = max(1, math.ceil(total_rows / per_page))
            page = max(1, min(page, pages))
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            df_sliced = df.iloc[start_idx:end_idx]
            
        return jsonify({
            "data": df_sliced.to_dict(orient='records'),
            "total_rows": total_rows,
            "pages": pages,
            "current_page": page,
            "per_page": per_page,
            "totals": totals
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/job-form/export', methods=['POST'])
@login_required
def job_form_export():
    data = request.get_json()
    filters = {
        'bus': data.get('bu'),
        'otypes': data.get('type'),
        'onames': data.get('name'),
        'feeders': data.get('feeder'),
        'dts': data.get('dt'),
        'ftype': data.get('form_type', 'Full')
    }
    columns = data.get('columns', [])
    try:
        df = job_form_service.get_job_form_data(filters, columns)
        if df.empty:
            return jsonify({"error": "No data found for selected filters"}), 404
            
        filename = generate_descriptive_filename("Job_Forms", filters)
        export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
        os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, filename)
        
        # Use xlsxwriter engine for robustness
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Job Form')
        
        if os.path.exists(filepath):
            return jsonify({"success": True, "filename": filename})
        else:
            return jsonify({"error": "File creation failed"}), 500
    except Exception as e:
        import logging
        logging.error(f"Job Form Export Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Export failed: {str(e)}"}), 500

@app.route('/api/performance/rank')
@login_required
def api_performance_rank():
    period = request.args.get('period', 'daily')
    otype = request.args.get('otype', 'Both')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    year = request.args.get('year')
    quarter = request.args.get('quarter', 'Full')
    show_all = request.args.get('all', 'false') == 'true'
    
    try:
        # Determine date range based on period
        now = datetime.now()
        if period == 'daily':
            start = now.strftime('%Y-%m-%d')
            end = start
        elif period == 'weekly':
            start = (now - pd.Timedelta(days=now.weekday())).strftime('%Y-%m-%d')
            end = now.strftime('%Y-%m-%d')
        elif period == 'monthly':
            start = now.strftime('%Y-%m-01')
            end = now.strftime('%Y-%m-%d')
        elif period == 'annual':
            if not year:
                year = str(now.year)
            
            if quarter == 'Full':
                start = f"{year}-01-01"
                end = f"{year}-12-31"
            elif quarter == 'Q1':
                start, end = f"{year}-01-01", f"{year}-03-31"
            elif quarter == 'Q2':
                start, end = f"{year}-04-01", f"{year}-06-30"
            elif quarter == 'Q3':
                start, end = f"{year}-07-01", f"{year}-09-30"
            elif quarter == 'Q4':
                start, end = f"{year}-10-01", f"{year}-12-31"
        elif period == 'custom':
            start = start_date
            end = end_date
        else:
            return jsonify({"error": "Invalid period"}), 400

        # Query all_payments joined with customers for ranking
        otype_clause = ""
        if otype != 'Both':
            otype_clause = "AND c.officer_type = :otype"

        sql = text(f"""
            SELECT 
                c.account_officer, 
                c.business_unit, 
                c.officer_type,
                SUM(p.amount_paid) as recovery
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            WHERE p.date_of_payment BETWEEN :start AND :end
            {otype_clause}
            GROUP BY c.account_officer, c.business_unit, c.officer_type
        """)
        
        params = {"start": start, "end": end}
        if otype != 'Both':
            params["otype"] = otype

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
            
        if df.empty:
            return jsonify({"officers": [], "units": []})

        # --- 1. Recovery Officer Aggregation (Always group by name) ---
        def aggregate_officer(group):
            unique_bus = group['business_unit'].unique()
            bu_display = ", ".join(unique_bus)
            # Format breakdown for tooltip/display
            breakdown = ", ".join([f"{bu}: ₦{val:,.0f}" for bu, val in zip(group['business_unit'], group['recovery'])])
            return pd.Series({
                'recovery': group['recovery'].sum(),
                'business_unit': bu_display,
                'breakdown': breakdown,
                'officer_type': group['officer_type'].iloc[0],
                'is_multi_bu': len(unique_bus) > 1
            })

        officer_df = df.groupby('account_officer').apply(aggregate_officer).reset_index()
        
        if show_all:
            officer_rank = officer_df.sort_values('recovery', ascending=False).to_dict(orient='records')
        else:
            officer_rank = officer_df.sort_values('recovery', ascending=False).head(10).to_dict(orient='records')
        
        # --- 2. Aggregate Rank (Business Unit View) ---
        # We always group by Business Unit for the second table to show geographical performance
        def summarize_officers(group):
            unique_offs = group['account_officer'].unique()
            # The breakdown now shows individual Account Officers/Vendors and their recovery
            breakdown = ", ".join([f"{off}: ₦{val:,.0f}" for off, val in zip(group['account_officer'], group['recovery'])])
            return pd.Series({
                'recovery': group['recovery'].sum(),
                'breakdown': breakdown,
                'is_multi_bu': len(unique_offs) > 1 # Flag to show info icon if multiple officers exist in this BU
            })
        
        agg_rank_df = df.groupby('business_unit').apply(summarize_officers).reset_index()
        agg_rank = agg_rank_df.sort_values('recovery', ascending=False).head(5).to_dict(orient='records')
        
        # Determine the display label for the second table
        if otype == 'Vendor':
            rank_label = "Business Unit" # Showing BU performance for Vendors
        else:
            rank_label = "Business Unit"
        
        return jsonify({
            "officers": officer_rank,
            "units": agg_rank,
            "rank_label": rank_label,
            "period_start": start,
            "period_end": end
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/performance/export', methods=['POST'])
@login_required
def api_performance_export():
    data = request.get_json()
    period = data.get('period', 'daily')
    otype = data.get('otype', 'Both')
    start_date = data.get('start')
    end_date = data.get('end')
    year = data.get('year')
    quarter = data.get('quarter', 'Full')
    
    try:
        # Determine date range based on period (Reusing logic from api_performance_rank)
        now = datetime.now()
        if period == 'daily':
            start, end = now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d')
        elif period == 'weekly':
            start = (now - pd.Timedelta(days=now.weekday())).strftime('%Y-%m-%d')
            end = now.strftime('%Y-%m-%d')
        elif period == 'monthly':
            start, end = now.strftime('%Y-%m-01'), now.strftime('%Y-%m-%d')
        elif period == 'annual':
            if not year: year = str(now.year)
            if quarter == 'Full': start, end = f"{year}-01-01", f"{year}-12-31"
            elif quarter == 'Q1': start, end = f"{year}-01-01", f"{year}-03-31"
            elif quarter == 'Q2': start, end = f"{year}-04-01", f"{year}-06-30"
            elif quarter == 'Q3': start, end = f"{year}-07-01", f"{year}-09-30"
            elif quarter == 'Q4': start, end = f"{year}-10-01", f"{year}-12-31"
        elif period == 'custom':
            start, end = start_date, end_date
        else:
            return jsonify({"error": "Invalid period"}), 400

        # Query
        otype_clause = "AND c.officer_type = :otype" if otype != 'Both' else ""
        sql = text(f"""
            SELECT 
                c.account_officer as 'Officer Name', 
                c.business_unit as 'Business Unit', 
                c.officer_type as 'Officer Type',
                SUM(p.amount_paid) as 'Total Recovery'
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            WHERE p.date_of_payment BETWEEN :start AND :end
            {otype_clause}
            GROUP BY c.account_officer, c.business_unit, c.officer_type
            ORDER BY 'Total Recovery' DESC
        """)
        
        params = {"start": start, "end": end}
        if otype != 'Both': params["otype"] = otype

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
            
        if df.empty:
            return jsonify({"error": "No data found to export"}), 404

        # Grouping logic for final export (Summing up multi-BU recovery)
        final_df = df.groupby(['Officer Name', 'Officer Type']).agg({
            'Total Recovery': 'sum',
            'Business Unit': lambda x: ", ".join(x.unique())
        }).reset_index()
        
        final_df = final_df.sort_values('Total Recovery', ascending=False)
        final_df['Rank'] = range(1, len(final_df) + 1)
        
        # Reorder columns
        final_df = final_df[['Rank', 'Officer Name', 'Business Unit', 'Officer Type', 'Total Recovery']]
        
        filename = generate_descriptive_filename("Performance_Ranking", {
            "otype": otype,
            "period": period,
            "start": start,
            "end": end,
            "year": year,
            "quarter": quarter
        })
        export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
        os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, filename)
        
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='Officer Ranking')
            
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        import logging
        logging.error(f"Performance Export Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/payments/preview')
@login_required
def api_payments_preview():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    onames = request.args.getlist('officer')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    
    page = int(request.args.get('page', 1))
    per_page_arg = request.args.get('per_page', '50')
    
    if not bus or not otypes or not onames:
        return jsonify({
            "data": [],
            "total_rows": 0,
            "pages": 0,
            "current_page": 1,
            "per_page": 50,
            "totals": {}
        })

    try:
        sql = text("""
            SELECT 
                p.date_of_payment, 
                p.account_number, 
                c.account_name, 
                p.amount_paid, 
                c.account_officer, 
                c.business_unit,
                c.officer_type,
                c.account_address,
                c.undertaking,
                c.dt_name,
                c.closing_balance,
                COALESCE(afs.total_discounts, 0) as total_discount,
                COALESCE(afs.total_adjustments, 0) as total_adjustment,
                COALESCE(afs.outstanding_balance, 0) as outstanding_balance,
                COALESCE(afs.payment_plan, 'No') as payment_plan
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            LEFT JOIN account_financial_summary afs ON p.account_number = afs.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            AND p.date_of_payment BETWEEN :start AND :end
            ORDER BY p.date_of_payment DESC
        """)
        
        with engine.connect() as conn:
            results = conn.execute(sql, {
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames),
                "start": start_date,
                "end": end_date
            }).fetchall()
            
        records = [dict(r._mapping) for r in results]
        
        # Calculate Grand Totals
        total_amount_paid = sum(float(r.get('amount_paid') or 0) for r in records)
        totals = {
            "amount_paid": total_amount_paid
        }
        
        total_rows = len(records)
        
        if per_page_arg.lower() == 'all':
            per_page = total_rows
            pages = 1
            sliced_records = records
        else:
            per_page = int(per_page_arg)
            import math
            pages = max(1, math.ceil(total_rows / per_page))
            page = max(1, min(page, pages))
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            sliced_records = records[start_idx:end_idx]
            
        return jsonify({
            "data": sliced_records,
            "total_rows": total_rows,
            "pages": pages,
            "current_page": page,
            "per_page": per_page,
            "totals": totals
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/payments/export', methods=['POST'])
@login_required
def api_payments_export():
    data = request.get_json()
    bus = data.get('bu', [])
    otypes = data.get('type', [])
    onames = data.get('officer', [])
    start_date = data.get('start')
    end_date = data.get('end')
    
    if not bus or not onames:
        return jsonify({"error": "Missing filters"}), 400

    try:
        sql = text("""
            SELECT 
                p.account_number as 'Account Number', 
                c.account_name as 'Account Name', 
                c.account_address as 'Account Address',
                c.business_unit as 'Business Unit', 
                c.undertaking as 'Undertaking', 
                c.dt_name as 'DT Name', 
                c.closing_balance as 'Closing Balance',
                p.amount_paid as 'Amount Paid', 
                p.date_of_payment as 'Date of Payment', 
                COALESCE(afs.total_discounts, 0) as 'Total Discount', 
                COALESCE(afs.total_adjustments, 0) as 'Total Adjustment', 
                (SELECT SUM(op.amount_paid) FROM other_payments op 
                 WHERE op.account_number = p.account_number 
                 AND op.date_of_payment BETWEEN :start AND :end) as 'Other Payment',
                COALESCE(afs.outstanding_balance, 0) as 'Outstanding Balance', 
                COALESCE(afs.payment_plan, 'No') as 'Payment Plan (Yes/No)',
                c.account_officer as 'Account Officer'
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            LEFT JOIN account_financial_summary afs ON p.account_number = afs.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            AND p.date_of_payment BETWEEN :start AND :end
            ORDER BY p.date_of_payment DESC
        """)
        
        with engine.connect() as conn:
            # Explicitly cast to list for SQLAlchemy IN clause safety
            df = pd.read_sql(sql, conn, params={
                "bus": list(bus),
                "otypes": list(otypes),
                "onames": list(onames),
                "start": start_date,
                "end": end_date
            })
            
        if df.empty:
            return jsonify({"error": "No data found for selected criteria"}), 404
            
        filename = generate_descriptive_filename("Payment_Listing", {
            "bu": bus,
            "otypes": otypes,
            "start": start_date,
            "end": end_date
        })
        export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
        os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, filename)
        
        # Use xlsxwriter engine for robustness
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Payments')
            
        if os.path.exists(filepath):
            return jsonify({"success": True, "filename": filename})
        else:
            return jsonify({"error": "Failed to generate Excel file"}), 500
    except Exception as e:
        import logging
        logging.error(f"Payment Export Error: {str(e)}")
        return jsonify({"error": f"Export failed: {str(e)}"}), 500

@app.route('/api/customers/preview')
@login_required
def api_customers_preview():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    onames = request.args.getlist('officer')
    
    page = int(request.args.get('page', 1))
    per_page_arg = request.args.get('per_page', '50')
    
    if not bus or not otypes or not onames:
        return jsonify({
            "data": [],
            "total_rows": 0,
            "pages": 0,
            "current_page": 1,
            "per_page": 50,
            "totals": {}
        })

    try:
        # Complex query to get all requested fields, aligning with main_app.py standards
        sql = text("""
            SELECT 
                c.account_number, 
                c.account_name, 
                c.account_address, 
                c.business_unit, 
                c.undertaking,
                c.account_officer, 
                c.feeder as feeder_name,
                c.dt_name,
                v.phone_number,
                c.closing_balance,
                COALESCE(p.total_payments, 0) as total_payments,
                COALESCE(d.total_discounts_approved, 0) as total_discounts_approved,
                COALESCE(a.total_adjustments_approved, 0) as total_adjustments_approved,
                lp.last_payment_date
            FROM customers c
            LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments GROUP BY account_number) p ON c.account_number = p.account_number
            LEFT JOIN (SELECT account_number, MAX(date_of_payment) as last_payment_date FROM collections GROUP BY account_number) lp ON c.account_number = lp.account_number
            LEFT JOIN (
                SELECT account_number, 
                    SUM(CASE WHEN LOWER(status) = 'approved' OR LOWER(user_who_approved) LIKE '%okoye%' OR LOWER(user_who_approved) LIKE '%forstinus%' THEN discounted_amount ELSE 0 END) as total_discounts_approved
                FROM discounts GROUP BY account_number
            ) d ON c.account_number = d.account_number
            LEFT JOIN (
                SELECT account_number, 
                    SUM(CASE WHEN LOWER(status) = 'approved' OR LOWER(user_who_approved_adjustment) LIKE '%okoye%' OR LOWER(user_who_approved_adjustment) LIKE '%forstinus%' THEN adjustment_amount ELSE 0 END) as total_adjustments_approved
                FROM adjustments GROUP BY account_number
            ) a ON c.account_number = a.account_number
            LEFT JOIN (SELECT account_number, phone_number FROM validation WHERE id IN (SELECT MAX(id) FROM validation GROUP BY account_number)) v ON c.account_number = v.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            ORDER BY c.closing_balance DESC
        """)
        
        with engine.connect() as conn:
            results = conn.execute(sql, {
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames)
            }).fetchall()
            
        processed_data = []
        for r in results:
            row = dict(r._mapping)
            closing = float(row['closing_balance'] or 0)
            payments = float(row['total_payments'] or 0)
            discounts = float(row['total_discounts_approved'] or 0)
            adjustments = float(row['total_adjustments_approved'] or 0)
            
            # Outstanding Balance
            row['outstanding_balance'] = closing - payments - discounts - adjustments
            is_pp = (payments >= 0.3 * closing) and (row['outstanding_balance'] > 0)
            row['pp_status'] = "Yes" if is_pp else "No"
            
            processed_data.append(row)
            
        # Calculate Grand Totals
        total_payments = sum(float(r.get('total_payments') or 0) for r in processed_data)
        total_outstanding = sum(float(r.get('outstanding_balance') or 0) for r in processed_data)
        totals = {
            "total_payments": total_payments,
            "outstanding_balance": total_outstanding
        }
        
        total_rows = len(processed_data)
        
        if per_page_arg.lower() == 'all':
            per_page = total_rows
            pages = 1
            sliced_data = processed_data
        else:
            per_page = int(per_page_arg)
            import math
            pages = max(1, math.ceil(total_rows / per_page))
            page = max(1, min(page, pages))
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            sliced_data = processed_data[start_idx:end_idx]
            
        return jsonify({
            "data": sliced_data,
            "total_rows": total_rows,
            "pages": pages,
            "current_page": page,
            "per_page": per_page,
            "totals": totals
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/customers/export', methods=['POST'])
@login_required
def api_customers_export():
    data = request.get_json()
    bus = data.get('bu', [])
    otypes = data.get('type', [])
    onames = data.get('officer', [])
    
    if not bus or not onames:
        return jsonify({"error": "Missing filters"}), 400

    try:
        sql = text("""
            SELECT 
                c.account_number as 'Account Number', 
                c.account_name as 'Account Name', 
                c.account_address as 'Account Address', 
                c.business_unit as 'Business Unit', 
                c.undertaking as 'Undertaking',
                c.account_officer as 'Account Officer', 
                c.feeder as 'Feeder Name',
                c.dt_name as 'DT Name',
                v.phone_number as 'Phone Number',
                c.closing_balance as 'Closing Balance',
                COALESCE(p.total_payments, 0) as total_payments,
                COALESCE(d.total_discounts_approved, 0) as total_discounts_approved,
                COALESCE(a.total_adjustments_approved, 0) as total_adjustments_approved,
                lp.last_payment_date as 'Last Payment Date'
            FROM customers c
            LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments GROUP BY account_number) p ON c.account_number = p.account_number
            LEFT JOIN (SELECT account_number, MAX(date_of_payment) as last_payment_date FROM collections GROUP BY account_number) lp ON c.account_number = lp.account_number
            LEFT JOIN (
                SELECT account_number, 
                    SUM(CASE WHEN LOWER(status) = 'approved' OR LOWER(user_who_approved) LIKE '%okoye%' OR LOWER(user_who_approved) LIKE '%forstinus%' THEN discounted_amount ELSE 0 END) as total_discounts_approved
                FROM discounts GROUP BY account_number
            ) d ON c.account_number = d.account_number
            LEFT JOIN (
                SELECT account_number, 
                    SUM(CASE WHEN LOWER(status) = 'approved' OR LOWER(user_who_approved_adjustment) LIKE '%okoye%' OR LOWER(user_who_approved_adjustment) LIKE '%forstinus%' THEN adjustment_amount ELSE 0 END) as total_adjustments_approved
                FROM adjustments GROUP BY account_number
            ) a ON c.account_number = a.account_number
            LEFT JOIN (SELECT account_number, phone_number FROM validation WHERE id IN (SELECT MAX(id) FROM validation GROUP BY account_number)) v ON c.account_number = v.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            ORDER BY c.closing_balance DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={
                "bus": list(bus),
                "otypes": list(otypes),
                "onames": list(onames)
            })
            
        if df.empty:
            return jsonify({"error": "No data found for selected criteria"}), 404

        # Calculate derived fields in Pandas
        df['Total Payments'] = df['total_payments'].astype(float)
        df['Valid Discount Amount'] = df['total_discounts_approved'].astype(float)
        df['Valid Adjustment Amount'] = df['total_adjustments_approved'].astype(float)
        df['Closing Balance'] = df['Closing Balance'].astype(float)
        
        df['Outstanding Balance'] = df['Closing Balance'] - df['Total Payments'] - df['Valid Discount Amount'] - df['Valid Adjustment Amount']
        
        def get_pp_status(row):
            is_pp = (row['Total Payments'] >= 0.3 * row['Closing Balance']) and (row['Outstanding Balance'] > 0)
            return "Yes" if is_pp else "No"

        df['Current Payment-Plan Status'] = df.apply(get_pp_status, axis=1)
        
        export_df = df[[
            'Account Number', 'Account Name', 'Account Address', 'Business Unit', 
            'Undertaking', 'Account Officer', 'Feeder Name', 'DT Name', 'Phone Number',
            'Closing Balance', 'Total Payments', 'Valid Discount Amount', 
            'Valid Adjustment Amount', 'Outstanding Balance', 'Last Payment Date', 
            'Current Payment-Plan Status'
        ]]
            
        filename = generate_descriptive_filename("Customer_Listing", {
            "bu": bus,
            "otypes": otypes
        })
        export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
        os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, filename)
        
        # Use xlsxwriter engine for robustness
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            export_df.to_excel(writer, index=False, sheet_name='Customers')
            
        if os.path.exists(filepath):
            return jsonify({"success": True, "filename": filename})
        else:
            return jsonify({"error": "Failed to create export file"}), 500
    except Exception as e:
        import logging
        logging.error(f"Customer Export Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Export failed: {str(e)}"}), 500

@app.route('/download/<filename>')
@login_required
def download_file(filename):
    export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
    return send_from_directory(export_dir, filename, as_attachment=True)


if __name__ == '__main__':
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
