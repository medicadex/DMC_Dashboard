import sys
import threading
import time
import os
import io
import math
import traceback
import logging
from functools import wraps
from datetime import datetime, timedelta

from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory, send_file, session, make_response, Response
from werkzeug.utils import secure_filename
from sqlalchemy import text
import pandas as pd

from db_utils import get_db_engine

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
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def generate_descriptive_filename(base_name, filters):
    """
    Generates a descriptive filename based on applied filters to exactly match main_app.py nomenclature.
    """
    
    # 1. Determine off_type
    otype = filters.get('otype') or filters.get('otypes')
    off_type = "All"
    if otype:
        if isinstance(otype, list):
            if len(otype) == 1:
                off_type = str(otype[0])
            elif len(otype) > 1:
                off_type = "Mixed_Types"
        else:
            if str(otype) != 'Both':
                off_type = str(otype)
            
    off_type = off_type.replace(' ', '_')
    report_name = str(base_name).replace(' ', '_')
    
    # 2. Determine dates
    start = filters.get('start')
    end = filters.get('end')
    
    if start and end:
        try:
            start_fmt = datetime.strptime(start, "%Y-%m-%d").strftime("%d-%m-%Y")
            end_fmt = datetime.strptime(end, "%Y-%m-%d").strftime("%d-%m-%Y")
        except Exception:
            start_fmt = start
            end_fmt = end
        
        filename = f"{off_type}_{report_name}_{start_fmt}_to_{end_fmt}"
    else:
        filename = f"{off_type}_{report_name}_Full"
        
    return secure_filename(f"{filename}.xlsx")

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

# In-memory cache for Excel export streaming (Memory leak prevention)
EXPORT_CACHE = {}  # filename -> {"data": bytes, "timestamp": datetime}

def cache_export_file(filename, data_bytes):
    """Caches Excel file bytes in memory and prunes entries older than 10 minutes."""
    now = datetime.now()
    # Prune expired entries to prevent memory leaks
    expired = [k for k, v in EXPORT_CACHE.items() if (now - v["timestamp"]).total_seconds() > 600]
    for k in expired:
        EXPORT_CACHE.pop(k, None)
    EXPORT_CACHE[filename] = {"data": data_bytes, "timestamp": now}

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
            logger.error(f"Login error: {e}", exc_info=True)
            return jsonify({'success': False, 'message': 'An error occurred during authentication.'}), 401
    return render_template('login.html')

@app.route('/vendor-login', methods=['GET', 'POST'])
def vendor_login():
    if request.method == 'POST':
        data = request.get_json()
        vendor_name = data.get('vendor_name', '').strip()
        
        if len(vendor_name) < 3:
            return jsonify({'success': False, 'message': 'Vendor name must be at least 3 characters long.'}), 400
            
        try:
            prefix = f"{vendor_name[:3].lower()}%"
            sql = text("""
                SELECT DISTINCT account_officer 
                FROM customers 
                WHERE officer_type = 'Vendor' AND LOWER(account_officer) LIKE :p 
                ORDER BY account_officer ASC
            """)
            with engine.connect() as conn:
                rows = conn.execute(sql, {"p": prefix}).fetchall()
                
            matches = [r[0] for r in rows if r[0]]
            
            if not matches:
                # Log fail
                staff_repo.log_activity(vendor_name, "VENDOR_LOGIN_FAILED", f"No vendor match for prefix '{vendor_name[:3]}'", event_type='MAJOR')
                return jsonify({'success': False, 'message': 'Vendor agency name or prefix code not recognized.'}), 401
                
            # If the user entered an exact match case-insensitive, prioritize it
            exact_match = None
            for m in matches:
                if m.lower() == vendor_name.lower():
                    exact_match = m
                    break
                    
            if exact_match:
                matched_name = exact_match
            elif len(matches) == 1:
                matched_name = matches[0]
            else:
                return jsonify({
                    'success': False,
                    'multiple_matches': True,
                    'matches': matches,
                    'message': 'Multiple matching vendors found. Please select yours.'
                })
                
            # Log success
            staff_repo.log_activity(matched_name, "VENDOR_LOGIN_SUCCESS", f"Matched prefix '{vendor_name[:3]}' to '{matched_name}'", event_type='MAJOR')
            
            # Bind session
            session['user'] = {
                "id": 0,
                "username": matched_name,
                "full_name": matched_name,
                "role": "Vendor"
            }
            session_manager.update_activity()
            return jsonify({'success': True, 'vendor': matched_name})
        except Exception as e:
            logger.error(f"Vendor login error: {e}", exc_info=True)
            return jsonify({'success': False, 'message': 'An internal error occurred during authentication.'}), 500
            
    return render_template('vendor_login.html')

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
    if session.get('user') and session['user']['role'] == 'Vendor':
        return redirect(url_for('dashboard'))
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
                logger.error(f"Uploader process failed: {e}", exc_info=True)
                return jsonify({"error": "An internal error occurred while processing the uploaded file."}), 500
                
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
                logger.error(f"Admin upload failed for '{table_name}': {e}", exc_info=True)
                err_msg = str(e) if isinstance(e, (ValueError, KeyError)) else "Internal processing or database error."
                errors.append({"table": table_name, "error": err_msg})
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
                    CEILING((DAY(p.date_of_payment) + WEEKDAY(p.date_of_payment - INTERVAL DAY(p.date_of_payment)-1 DAY)) / 7) as week_num,
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
        logger.error(f"Dashboard stats error: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An internal error occurred while retrieving dashboard statistics.'}), 500

@app.route('/api/autocomplete')
@login_required
def api_autocomplete():
    query = request.args.get('q', '').strip()
    if len(query) < 3:
        return jsonify([])

    search_term = f"%{query}%"
    
    if session['user']['role'] == 'Vendor':
        sql = text("""
            SELECT account_number, account_name, account_address
            FROM customers
            WHERE account_officer = :vendor
              AND (account_number LIKE :q OR account_name LIKE :q OR account_address LIKE :q)
            LIMIT 30
        """)
        params = {"q": search_term, "vendor": session['user']['username']}
    else:
        sql = text("""
            SELECT account_number, account_name, account_address
            FROM customers
            WHERE account_number LIKE :q OR account_name LIKE :q OR account_address LIKE :q
            LIMIT 30
        """)
        params = {"q": search_term}
    
    with engine.connect() as conn:
        results = conn.execute(sql, params).fetchall()
        
    return jsonify([dict(r._mapping) for r in results])

@app.route('/api/account_dashboard/<account_number>')
@login_required
def api_account_dashboard(account_number):
    try:
        # Enforce Vendor role data isolation at the lookup boundary
        if session['user']['role'] == 'Vendor':
            sql = text("SELECT account_officer FROM customers WHERE account_number = :acc")
            with engine.connect() as conn:
                row = conn.execute(sql, {"acc": account_number}).fetchone()
            if not row or row[0] != session['user']['username']:
                return jsonify({"error": "Access denied. This account is not assigned to your agency."}), 403
                
        data = account_service.get_account_financials(account_number, session['user']['username'], session['user']['role'], force_online=True)
        if not data:
            return jsonify({"error": "Account not found"}), 404
        return jsonify(data)
    except Exception as e:
        logger.error(f"Account dashboard error for {account_number}: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while retrieving account details."}), 500

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
        logger.error(f"Update start date error: {e}", exc_info=True)
        return jsonify({"success": False, "message": "An internal error occurred while updating the officer start date."}), 500

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
        logger.error(f"Update officer dates batch error: {e}", exc_info=True)
        return jsonify({"success": False, "message": "An internal error occurred while processing the batch update."}), 500

@app.route('/api/job-form/initial-data')
@login_required
def job_form_initial_data():
    try:
        if session['user']['role'] == 'Vendor':
            sql = text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != '' ORDER BY business_unit ASC")
            with engine.connect() as conn:
                res = conn.execute(sql, {"v": session['user']['username']}).fetchall()
                bus = [r[0] for r in res]
        else:
            bus = job_form_service.get_distinct_values("customers", "business_unit")
        return jsonify({"business_units": bus})
    except Exception as e:
        logger.error(f"Job form initial data error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while retrieving job form configuration."}), 500

@app.route('/api/job-form/undertakings')
@login_required
def job_form_undertakings():
    bus = request.args.getlist('bu')
    if not bus:
        return jsonify([])
    try:
        if session['user']['role'] == 'Vendor':
            sql = text("""
                SELECT DISTINCT undertaking 
                FROM customers 
                WHERE business_unit IN :bus AND account_officer = :v 
                  AND undertaking IS NOT NULL AND undertaking != '' 
                ORDER BY undertaking ASC
            """)
            with engine.connect() as conn:
                res = conn.execute(sql, {"bus": tuple(bus), "v": session['user']['username']}).fetchall()
                undertakings = [r[0] for r in res]
        else:
            undertakings = job_form_service.get_undertakings(bus)
        return jsonify(undertakings)
    except Exception as e:
        logger.error(f"Job form undertakings error: {e}", exc_info=True)
        return jsonify([])

@app.route('/api/job-form/officers')
@login_required
def job_form_officers():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    undertakings = request.args.getlist('undertaking')
    if not bus or not otypes:
        return jsonify([])
    try:
        if session['user']['role'] == 'Vendor':
            names = [session['user']['username']]
        else:
            names = job_form_service.get_officer_names(bus, otypes, undertakings=undertakings)
        return jsonify(names)
    except Exception as e:
        logger.error(f"Job form officers error: {e}", exc_info=True)
        return jsonify([])

@app.route('/api/job-form/feeders')
@login_required
def job_form_feeders():
    bus = request.args.getlist('bu')
    names = request.args.getlist('name')
    undertakings = request.args.getlist('undertaking')
    if not bus or not names:
        return jsonify([])
    try:
        if session['user']['role'] == 'Vendor':
            ut_clause = "AND undertaking IN :uts" if undertakings else ""
            sql_str = f"""
                SELECT DISTINCT feeder 
                FROM customers 
                WHERE business_unit IN :bus AND account_officer = :v {ut_clause}
                  AND feeder IS NOT NULL AND feeder != '' 
                ORDER BY feeder ASC
            """
            params = {"bus": tuple(bus), "v": session['user']['username']}
            if undertakings:
                params["uts"] = tuple(undertakings)
            with engine.connect() as conn:
                res = conn.execute(text(sql_str), params).fetchall()
                feeders = [r[0] for r in res]
        else:
            feeders = job_form_service.get_feeders(bus, names, undertakings=undertakings)
        return jsonify(feeders)
    except Exception as e:
        logger.error(f"Job form feeders error: {e}", exc_info=True)
        return jsonify([])

@app.route('/api/job-form/dts')
@login_required
def job_form_dts():
    bus = request.args.getlist('bu')
    feeders = request.args.getlist('feeder')
    undertakings = request.args.getlist('undertaking')
    if not bus or not feeders:
        return jsonify([])
    try:
        if session['user']['role'] == 'Vendor':
            ut_clause = "AND undertaking IN :uts" if undertakings else ""
            sql_str = f"""
                SELECT DISTINCT dt_name 
                FROM customers 
                WHERE business_unit IN :bus AND account_officer = :v AND feeder IN :feeders {ut_clause}
                  AND dt_name IS NOT NULL AND dt_name != '' 
                ORDER BY dt_name ASC
            """
            params = {
                "bus": tuple(bus), 
                "v": session['user']['username'], 
                "feeders": tuple(feeders)
            }
            if undertakings:
                params["uts"] = tuple(undertakings)
            with engine.connect() as conn:
                res = conn.execute(text(sql_str), params).fetchall()
                dts = [r[0] for r in res]
        else:
            dts = job_form_service.get_dt_names(bus, feeders, undertakings=undertakings)
        return jsonify(dts)
    except Exception as e:
        logger.error(f"Job form DTs error: {e}", exc_info=True)
        return jsonify([])

@app.route('/api/job-form/preview')
@login_required
def job_form_preview():
    otypes_val = request.args.getlist('type')
    onames_val = request.args.getlist('name')
    bus_val = request.args.getlist('bu')
    
    if session['user']['role'] == 'Vendor':
        otypes_val = ['Vendor']
        onames_val = [session['user']['username']]
        if not bus_val:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != ''"), {"v": session['user']['username']}).fetchall()
                bus_val = [r[0] for r in res]
                
    filters = {
        'bus': bus_val,
        'undertakings': request.args.getlist('undertaking'),
        'otypes': otypes_val,
        'onames': onames_val,
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
        logger.error(f"Job form preview error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while generating the preview."}), 500

@app.route('/api/job-form/count')
@login_required
def job_form_count():
    otypes_val = request.args.getlist('type')
    onames_val = request.args.getlist('name')
    bus_val = request.args.getlist('bu')
    
    if session['user']['role'] == 'Vendor':
        otypes_val = ['Vendor']
        onames_val = [session['user']['username']]
        if not bus_val:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != ''"), {"v": session['user']['username']}).fetchall()
                bus_val = [r[0] for r in res]
                
    filters = {
        'bus': bus_val,
        'undertakings': request.args.getlist('undertaking'),
        'otypes': otypes_val,
        'onames': onames_val,
        'feeders': request.args.getlist('feeder'),
        'dts': request.args.getlist('dt'),
        'ftype': request.args.get('form_type', 'Full')
    }
    try:
        count = job_form_service.count_job_form_rows(filters)
        return jsonify({"count": count})
    except Exception as e:
        logger.error(f"Job Form Count Error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while counting rows."}), 500

@app.route('/api/job-form/export', methods=['POST'])
@login_required
def job_form_export():
    data = request.get_json()
    otypes_val = data.get('type', [])
    onames_val = data.get('name', [])
    bus_val = data.get('bu', [])
    
    if session['user']['role'] == 'Vendor':
        otypes_val = ['Vendor']
        onames_val = [session['user']['username']]
        if not bus_val:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != ''"), {"v": session['user']['username']}).fetchall()
                bus_val = [r[0] for r in res]
                
    filters = {
        'bus': bus_val,
        'undertakings': data.get('undertaking', []),
        'otypes': otypes_val,
        'onames': onames_val,
        'feeders': data.get('feeder', []),
        'dts': data.get('dt', []),
        'ftype': data.get('form_type', 'Full')
    }
    columns = data.get('columns', [])
    try:
        df = job_form_service.get_job_form_data(filters, columns)
        if df.empty:
            return jsonify({"error": "No data found for selected filters"}), 404
            
        # Rename columns to Pretty Titles for export to match main_app.py nomenclature
        pretty_names = {
            "account_number": "Account Number", 
            "account_name": "Name", 
            "account_address": "Address", 
            "closing_balance": "Closing Balance",
            "pos_other_payments": "POS/Other Payments",
            "adjustment": "Adjustment",
            "discount": "Discount",
            "outstanding_balance": "Outstanding Balance",
            "payment_plan_status": "Status",
            "last_payment_date": "Last Payment Date",
            "phone_number": "Phone",
            "dt_name": "DT Name",
            "feeder": "Feeder",
            "account_officer": "Account Officer"
        }
        actual_remapping = {k: v for k, v in pretty_names.items() if k in df.columns}
        df.rename(columns=actual_remapping, inplace=True)

        # Generate filename to exactly match job_form_ui.py nomenclature: {off_str}_{form_type}_{stamp}.xlsx
        officers = filters.get('onames') or []
        if not officers or len(officers) > 2:
            off_str = "MultipleOfficers"
        else:
            off_str = "_".join([o.replace(" ", "") for o in officers])
            
        form_type = filters.get('ftype', 'Full').replace(" ", "")
        stamp = datetime.now().strftime('%d-%m-%Y')
        filename = secure_filename(f"{off_str}_{form_type}_{stamp}.xlsx")
        
        # Stream Excel directly into in-memory buffer to prevent memory leaks and disk usage
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Job Form')
        buffer.seek(0)
        
        cache_export_file(filename, buffer.getvalue())
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        logger.error(f"Job Form Export Error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while exporting the job form report."}), 500


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
        vendor_clause = ""
        if otype != 'Both':
            otype_clause = "AND c.officer_type = :otype"
            
        if session['user']['role'] == 'Vendor':
            vendor_clause = "AND c.account_officer = :vendor_name"

        sql = text(f"""
            SELECT 
                c.account_officer, 
                c.business_unit, 
                c.officer_type,
                SUM(p.amount_paid) as recovery
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            WHERE DATE(p.date_of_payment) BETWEEN :start AND :end
            {otype_clause}
            {vendor_clause}
            GROUP BY c.account_officer, c.business_unit, c.officer_type
        """)
        
        params = {"start": start, "end": end}
        if otype != 'Both':
            params["otype"] = otype
        if session['user']['role'] == 'Vendor':
            params["vendor_name"] = session['user']['username']

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
        logger.error(f"Performance rank query error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while calculating performance ranking."}), 500

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
        vendor_clause = "AND c.account_officer = :vendor_name" if session['user']['role'] == 'Vendor' else ""
        sql = text(f"""
            SELECT 
                c.account_officer as 'Officer Name', 
                c.business_unit as 'Business Unit', 
                c.officer_type as 'Officer Type',
                SUM(p.amount_paid) as 'Total Recovery'
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            WHERE DATE(p.date_of_payment) BETWEEN :start AND :end
            {otype_clause}
            {vendor_clause}
            GROUP BY c.account_officer, c.business_unit, c.officer_type
            ORDER BY `Total Recovery` DESC
        """)
        
        params = {"start": start, "end": end}
        if otype != 'Both': params["otype"] = otype
        if session['user']['role'] == 'Vendor':
            params["vendor_name"] = session['user']['username']

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
        
        filename = generate_descriptive_filename("Adv._Variance_Analysis", {
            "otype": otype,
            "period": period,
            "start": start,
            "end": end,
            "year": year,
            "quarter": quarter
        })
        
        # Stream Excel directly into in-memory buffer to prevent memory leaks and disk usage
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='Officer Ranking')
        buffer.seek(0)
        
        cache_export_file(filename, buffer.getvalue())
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        logger.error(f"Performance Export Error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while exporting performance ranking."}), 500

@app.route('/api/payments/preview')
@login_required
def api_payments_preview():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    onames = request.args.getlist('officer')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    
    if session['user']['role'] == 'Vendor':
        otypes = ['Vendor']
        onames = [session['user']['username']]
        if not bus:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != ''"), {"v": session['user']['username']}).fetchall()
                bus = [r[0] for r in res]
                
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
        # Calculate Grand Totals and Total Rows globally using SQL
        count_sql = text("""
            SELECT 
                COUNT(*) as total_rows,
                SUM(p.amount_paid) as total_amount
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            AND DATE(p.date_of_payment) BETWEEN :start AND :end
        """)
        
        with engine.connect() as conn:
            params = {
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames),
                "start": start_date,
                "end": end_date
            }
            count_res = conn.execute(count_sql, params).fetchone()
            total_rows = int(count_res[0]) if count_res and count_res[0] else 0
            total_amount_paid = float(count_res[1]) if count_res and count_res[1] else 0.0
            
        totals = {
            "amount_paid": total_amount_paid
        }
        if per_page_arg.lower() == 'all':
            per_page = total_rows if total_rows > 0 else 50
            pages = 1
            limit_clause = ""
        else:
            per_page = int(per_page_arg)
            pages = max(1, math.ceil(total_rows / per_page)) if total_rows > 0 else 1
            page = max(1, min(page, pages))
            offset = (page - 1) * per_page
            limit_clause = f" LIMIT {per_page} OFFSET {offset}"

        sql = text(f"""
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
            AND DATE(p.date_of_payment) BETWEEN :start AND :end
            ORDER BY p.date_of_payment DESC
            {limit_clause}
        """)
        
        with engine.connect() as conn:
            results = conn.execute(sql, params).fetchall()
            
        records = []
        for r in results:
            row = dict(r._mapping)
            dop = row.get('date_of_payment')
            if dop:
                if isinstance(dop, (datetime, pd.Timestamp)):
                    row['date_of_payment'] = dop.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    row['date_of_payment'] = str(dop)
            else:
                row['date_of_payment'] = ''
            records.append(row)
            
        return jsonify({
            "data": records,
            "total_rows": total_rows,
            "pages": pages,
            "current_page": page,
            "per_page": per_page,
            "totals": totals
        })
    except Exception as e:
        logger.error(f"Payments preview query error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while generating the payments preview."}), 500

@app.route('/api/payments/export', methods=['POST'])
@login_required
def api_payments_export():
    data = request.get_json()
    bus = data.get('bu', [])
    otypes = data.get('type', [])
    onames = data.get('officer', [])
    start_date = data.get('start')
    end_date = data.get('end')
    
    if session['user']['role'] == 'Vendor':
        otypes = ['Vendor']
        onames = [session['user']['username']]
        if not bus:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != ''"), {"v": session['user']['username']}).fetchall()
                bus = [r[0] for r in res]
                
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
                 AND DATE(op.date_of_payment) BETWEEN :start AND :end) as 'Other Payment',
                COALESCE(afs.outstanding_balance, 0) as 'Outstanding Balance', 
                COALESCE(afs.payment_plan, 'No') as 'Payment Plan (Yes/No)',
                c.account_officer as 'Account Officer'
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            LEFT JOIN account_financial_summary afs ON p.account_number = afs.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            AND DATE(p.date_of_payment) BETWEEN :start AND :end
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

        # Convert the 'Date of Payment' column to a clean string format %Y-%m-%d %H:%M:%S before Excel export
        if 'Date of Payment' in df.columns:
            df['Date of Payment'] = pd.to_datetime(df['Date of Payment']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
        filename = generate_descriptive_filename("Payment_Listing", {
            "bu": bus,
            "otypes": otypes,
            "start": start_date,
            "end": end_date
        })
        
        # Stream Excel directly into in-memory buffer to prevent memory leaks and disk usage
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Payments')
        buffer.seek(0)
        
        cache_export_file(filename, buffer.getvalue())
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        logger.error(f"Payment Export Error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while exporting the payments report."}), 500

@app.route('/api/customers/preview')
@login_required
def api_customers_preview():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    onames = request.args.getlist('officer')
    
    if session['user']['role'] == 'Vendor':
        otypes = ['Vendor']
        onames = [session['user']['username']]
        if not bus:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != ''"), {"v": session['user']['username']}).fetchall()
                bus = [r[0] for r in res]
    
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
        # Calculate Grand Totals and Total Rows globally using SQL
        count_sql = text("""
            SELECT 
                COUNT(*) as total_rows,
                SUM(COALESCE(afs.total_payments, 0)) as sum_payments,
                SUM(c.closing_balance - COALESCE(afs.total_payments, 0) - COALESCE(afs.total_discounts, 0) - COALESCE(afs.total_adjustments, 0)) as sum_outstanding
            FROM customers c
            LEFT JOIN account_financial_summary afs ON c.account_number = afs.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
        """)
        
        with engine.connect() as conn:
            params = {
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames)
            }
            count_res = conn.execute(count_sql, params).fetchone()
            total_rows = int(count_res[0]) if count_res and count_res[0] else 0
            total_payments_global = float(count_res[1]) if count_res and count_res[1] else 0.0
            total_outstanding_global = float(count_res[2]) if count_res and count_res[2] else 0.0

        totals = {
            "total_payments": total_payments_global,
            "outstanding_balance": total_outstanding_global
        }
        if per_page_arg.lower() == 'all':
            per_page = total_rows if total_rows > 0 else 50
            pages = 1
            limit_clause = ""
        else:
            per_page = int(per_page_arg)
            pages = max(1, math.ceil(total_rows / per_page)) if total_rows > 0 else 1
            page = max(1, min(page, pages))
            offset = (page - 1) * per_page
            limit_clause = f" LIMIT {per_page} OFFSET {offset}"

        # Complex query to get all requested fields, aligning with main_app.py standards
        sql = text(f"""
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
                COALESCE(afs.total_payments, 0) as total_payments,
                COALESCE(afs.total_discounts, 0) as total_discounts_approved,
                COALESCE(afs.total_adjustments, 0) as total_adjustments_approved,
                lp.last_payment_date
            FROM customers c
            LEFT JOIN account_financial_summary afs ON c.account_number = afs.account_number
            LEFT JOIN (SELECT account_number, MAX(date_of_payment) as last_payment_date FROM collections GROUP BY account_number) lp ON c.account_number = lp.account_number
            LEFT JOIN (SELECT account_number, phone_number FROM validation WHERE id IN (SELECT MAX(id) FROM validation GROUP BY account_number)) v ON c.account_number = v.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            ORDER BY c.closing_balance DESC
            {limit_clause}
        """)
        
        with engine.connect() as conn:
            results = conn.execute(sql, params).fetchall()
            
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
            
        return jsonify({
            "data": processed_data,
            "total_rows": total_rows,
            "pages": pages,
            "current_page": page,
            "per_page": per_page,
            "totals": totals
        })
    except Exception as e:
        logger.error(f"Customers preview query error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while generating the customer preview."}), 500

@app.route('/api/customers/export', methods=['POST'])
@login_required
def api_customers_export():
    data = request.get_json()
    bus = data.get('bu', [])
    otypes = data.get('type', [])
    onames = data.get('officer', [])
    
    if session['user']['role'] == 'Vendor':
        otypes = ['Vendor']
        onames = [session['user']['username']]
        if not bus:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT business_unit FROM customers WHERE account_officer = :v AND business_unit IS NOT NULL AND business_unit != ''"), {"v": session['user']['username']}).fetchall()
                bus = [r[0] for r in res]
    
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
                COALESCE(afs.total_payments, 0) as total_payments,
                COALESCE(afs.total_discounts, 0) as total_discounts_approved,
                COALESCE(afs.total_adjustments, 0) as total_adjustments_approved,
                lp.last_payment_date as 'Last Payment Date'
            FROM customers c
            LEFT JOIN account_financial_summary afs ON c.account_number = afs.account_number
            LEFT JOIN (SELECT account_number, MAX(date_of_payment) as last_payment_date FROM collections GROUP BY account_number) lp ON c.account_number = lp.account_number
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
            
        filename = generate_descriptive_filename("Customer_Collection", {
            "bu": bus,
            "otypes": otypes
        })
        
        # Stream Excel directly into in-memory buffer to prevent memory leaks and disk usage
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            export_df.to_excel(writer, index=False, sheet_name='Customers')
        buffer.seek(0)
        
        cache_export_file(filename, buffer.getvalue())
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        logger.error(f"Customer Export Error: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred while exporting the customer report."}), 500

@app.route('/download/<filename>')
@login_required
def download_file(filename):
    # Retrieve Excel file from in-memory cache if available (memory leak & disk write prevention)
    cached = EXPORT_CACHE.pop(filename, None)
    if cached:
        buffer = io.BytesIO(cached["data"])
        return send_file(
            buffer,
            download_name=filename,
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    # Fallback to local disk uploads directory for older exports or other upload files
    export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
    filepath = os.path.join(export_dir, filename)
    if os.path.exists(filepath):
        return send_from_directory(export_dir, filename, as_attachment=True)
        
    logger.warning(f"Download requested for non-existent or expired file: {filename}")
    return jsonify({"error": "File not found or has expired."}), 404


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
