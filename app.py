from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory, session
from db_utils import get_db_engine
from datetime import datetime
from sqlalchemy import text
import os
import pandas as pd
from functools import wraps

from services.account_service import AccountService
from services.validation_service import ValidationService
from repositories.staff_repo import StaffRepository
from services.job_form_service import JobFormService
from services.auth_service import AuthService
from services.upload_service import UploadService
from services.reporting_service import ReportingService
from services.admin_report_service import AdminReportService
from services.export_service import ExportService

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
engine = get_db_engine()
staff_repo = StaffRepository(engine)
validation_service = ValidationService(engine)
account_service = AccountService(engine, staff_repo, validation_service)
job_form_service = JobFormService(engine)
auth_service = AuthService(staff_repo)
upload_service = UploadService(engine)
reporting_service = ReportingService(engine, staff_repo)
admin_report_service = AdminReportService(engine)
export_service = ExportService(engine)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        user_data = auth_service.login(username, password)
        if user_data:
            session['user'] = user_data
            return jsonify({'success': True, 'user': user_data})
        return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', active_page='dashboard')

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

@app.route('/report-uploader')
@login_required
def report_uploader():
    return render_template('upload.html', active_page='upload')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'ie_logo.png', mimetype='image/png')

@app.route('/manifest.json')
def serve_manifest():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'manifest.json')

@app.route('/service-worker.js')
def serve_sw():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'service-worker.js')

# --- API ENDPOINTS ---

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
    columns = ["account_number", "account_name", "account_address", "closing_balance", "outstanding_balance", "account_officer"]
    try:
        df = job_form_service.get_job_form_data(filters, columns)
        if df.empty:
            return jsonify([])
        return jsonify(df.head(100).to_dict(orient='records'))
    except Exception as e:
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
            
        filename = f"Job_Form_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        df.to_excel(filepath, index=False)
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/performance/rank')
@login_required
def api_performance_rank():
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        data = reporting_service.get_performance_metrics(today)
        # Sort and take top 5 for officers and BUs
        # data is usually a DataFrame or list of dicts from ReportingService
        if isinstance(data, pd.DataFrame):
            # Officer Rank
            officer_rank = data.sort_values('MTD Recovery', ascending=False).head(10).to_dict(orient='records')
            # BU Rank
            bu_rank = data.groupby('Business Unit')['MTD Recovery'].sum().reset_index().sort_values('MTD Recovery', ascending=False).head(5).to_dict(orient='records')
            return jsonify({
                "officers": officer_rank,
                "units": bu_rank
            })
        return jsonify({"officers": [], "units": []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download/<filename>')
@login_required
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
