import sys
import threading
import time
import os
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory, session
from werkzeug.utils import secure_filename
from db_utils import get_db_engine
from datetime import datetime
from sqlalchemy import text
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
from utils.security import SessionManager
from db_utils import get_db_engine

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
session_manager = SessionManager(timeout_minutes=60)
auth_service = AuthService(staff_repo, session_manager)
upload_service = UploadService(engine, staff_repo)
reporting_service = ReportingService(engine, staff_repo)
admin_report_service = AdminReportService(engine, staff_repo)
export_service = ExportService(engine)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

@app.after_request
def add_header(response):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 0 seconds.
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response

# --- ROUTES ---

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
        # Ensure path is absolute and directory exists
        export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
        os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, filename)
        
        print(f"Exporting Job Form to: {filepath}")
        df.to_excel(filepath, index=False)
        
        if os.path.exists(filepath):
            return jsonify({"success": True, "filename": filename})
        else:
            return jsonify({"error": "File was not created on server"}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

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

@app.route('/api/payments/preview')
@login_required
def api_payments_preview():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    onames = request.args.getlist('officer')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    
    if not bus or not otypes or not onames:
        return jsonify([])

    try:
        sql = text("""
            SELECT 
                p.date_of_payment, 
                p.account_number, 
                c.account_name, 
                p.amount_paid, 
                c.account_officer, 
                c.business_unit,
                c.officer_type
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            AND p.date_of_payment BETWEEN :start AND :end
            ORDER BY p.date_of_payment DESC
            LIMIT 500
        """)
        
        with engine.connect() as conn:
            results = conn.execute(sql, {
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames),
                "start": start_date,
                "end": end_date
            }).fetchall()
            
        return jsonify([dict(r._mapping) for r in results])
    except Exception as e:
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
                p.date_of_payment as 'Date', 
                p.account_number as 'Account #', 
                c.account_name as 'Customer Name', 
                p.amount_paid as 'Amount (N)', 
                c.account_officer as 'Officer', 
                c.business_unit as 'BU',
                c.officer_type as 'Type'
            FROM all_payments p
            JOIN customers c ON p.account_number = c.account_number
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            AND p.date_of_payment BETWEEN :start AND :end
            ORDER BY p.date_of_payment DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames),
                "start": start_date,
                "end": end_date
            })
            
        if df.empty:
            return jsonify({"error": "No data found"}), 404
            
        filename = f"Payment_Listing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
        filepath = os.path.join(export_dir, filename)
        
        df.to_excel(filepath, index=False)
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/customers/preview')
@login_required
def api_customers_preview():
    bus = request.args.getlist('bu')
    otypes = request.args.getlist('type')
    onames = request.args.getlist('officer')
    
    if not bus or not otypes or not onames:
        return jsonify([])

    try:
        # Complex query to get all requested fields
        sql = text("""
            SELECT 
                c.account_number, 
                c.account_name, 
                c.account_address, 
                c.business_unit, 
                c.account_officer, 
                c.closing_balance,
                COALESCE(p.total_payments, 0) as total_payments,
                COALESCE(o.total_other, 0) as total_other,
                COALESCE(d.total_discounts_display, 0) as total_discounts_display,
                COALESCE(d.total_discounts_approved, 0) as total_discounts_approved,
                COALESCE(a.total_adjustments_display, 0) as total_adjustments_display,
                COALESCE(a.total_adjustments_approved, 0) as total_adjustments_approved,
                coll.last_payment_date
            FROM customers c
            LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments GROUP BY account_number) p ON c.account_number = p.account_number
            LEFT JOIN (SELECT account_number, MAX(NULLIF(date_of_payment, '0000-00-00')) as last_payment_date FROM collections GROUP BY account_number) coll ON c.account_number = coll.account_number
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
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            ORDER BY c.closing_balance DESC
            LIMIT 500
        """)
        
        with engine.connect() as conn:
            results = conn.execute(sql, {
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames)
            }).fetchall()
            
        # Process results to calculate outstanding balance and payment plan status
        processed_data = []
        today = pd.Timestamp.now().normalize()
        
        for r in results:
            row = dict(r._mapping)
            closing = float(row['closing_balance'] or 0)
            payments = float(row['total_payments'] or 0)
            other = float(row['total_other'] or 0)
            
            # Display values (all non-rejected)
            discounts_display = float(row['total_discounts_display'] or 0)
            adjustments_display = float(row['total_adjustments_display'] or 0)
            
            # Approved values (for calculation)
            discounts_approved = float(row['total_discounts_approved'] or 0)
            adjustments_approved = float(row['total_adjustments_approved'] or 0)
            
            # 1. Total Payments
            row['total_payments_combined'] = payments + other
            
            # UI display fields
            row['discount'] = discounts_display
            row['adjustment'] = adjustments_display
            
            # 2. Outstanding Balance (Using approved only)
            row['outstanding_balance'] = closing - payments - discounts_approved - adjustments_approved
            
            # 3. Payment Plan Status (Using approved only)
            is_pp = (payments >= 0.3 * closing) and (row['outstanding_balance'] > 0)
            if not is_pp:
                row['pp_status'] = "No Plan"
            else:
                last_pay = row['last_payment_date']
                if not last_pay:
                    row['pp_status'] = "Defaulted"
                else:
                    try:
                        last_pay_dt = pd.to_datetime(last_pay)
                        diff = (today - last_pay_dt.normalize()).days
                        row['pp_status'] = "Active" if diff <= 30 else "Defaulted"
                    except:
                        row['pp_status'] = "Defaulted"
            
            processed_data.append(row)
            
        return jsonify(processed_data)
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
        # Use the same complex logic as preview for full parity
        sql = text("""
            SELECT 
                c.account_number, 
                c.account_name, 
                c.account_address, 
                c.business_unit, 
                c.account_officer, 
                c.closing_balance,
                COALESCE(p.total_payments, 0) as total_payments,
                COALESCE(o.total_other, 0) as total_other,
                COALESCE(d.total_discounts_display, 0) as total_discounts_display,
                COALESCE(d.total_discounts_approved, 0) as total_discounts_approved,
                COALESCE(a.total_adjustments_display, 0) as total_adjustments_display,
                COALESCE(a.total_adjustments_approved, 0) as total_adjustments_approved,
                coll.last_payment_date
            FROM customers c
            LEFT JOIN (SELECT account_number, SUM(amount_paid) as total_payments FROM all_payments GROUP BY account_number) p ON c.account_number = p.account_number
            LEFT JOIN (SELECT account_number, MAX(NULLIF(date_of_payment, '0000-00-00')) as last_payment_date FROM collections GROUP BY account_number) coll ON c.account_number = coll.account_number
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
            WHERE c.business_unit IN :bus
            AND c.officer_type IN :otypes
            AND c.account_officer IN :onames
            ORDER BY c.closing_balance DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={
                "bus": tuple(bus),
                "otypes": tuple(otypes),
                "onames": tuple(onames)
            })
            
        if df.empty:
            return jsonify({"error": "No data found"}), 404

        # Calculate additional fields for export
        today = pd.Timestamp.now().normalize()
        
        df['Total Payments'] = df['total_payments'] + df['total_other']
        df['Outstanding Balance'] = df['closing_balance'] - df['total_payments'] - df['total_discounts_approved'] - df['total_adjustments_approved']
        
        def get_status(row):
            is_pp = (row['total_payments'] >= 0.3 * row['closing_balance']) and (row['Outstanding Balance'] > 0)
            if not is_pp: return "No Plan"
            if not row['last_payment_date']: return "Defaulted"
            try:
                diff = (today - pd.to_datetime(row['last_payment_date']).normalize()).days
                return "Active" if diff <= 30 else "Defaulted"
            except: return "Defaulted"

        df['Payment Plan Status'] = df.apply(get_status, axis=1)
        
        # Select and rename for professional output
        export_df = df[[
            'account_number', 'account_name', 'business_unit', 'account_officer', 
            'Total Payments', 'Outstanding Balance', 'Payment Plan Status', 'last_payment_date',
            'total_discounts_display', 'total_adjustments_display'
        ]].rename(columns={
            'account_number': 'Account #',
            'account_name': 'Customer Name',
            'business_unit': 'BU',
            'account_officer': 'Officer',
            'last_payment_date': 'Last Payment Date',
            'total_discounts_display': 'Discount',
            'total_adjustments_display': 'Adjustment'
        })
            
        filename = f"Customer_Listing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
        filepath = os.path.join(export_dir, filename)
        
        export_df.to_excel(filepath, index=False)
        return jsonify({"success": True, "filename": filename})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/download/<filename>')
@login_required
def download_file(filename):
    export_dir = os.path.abspath(app.config['UPLOAD_FOLDER'])
    return send_from_directory(export_dir, filename, as_attachment=True)

# --- LAUNCHER LOGIC ---

def start_flask():
    # Run the flask app on port 5000 internally. Wait to avoid thread blocking
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)

def main():
    if "--web" in sys.argv:
        print("Starting in Web Server Mode (Chrome/Hosted)")
        app.run(host="0.0.0.0", port=5000, debug=True)
    else:
        try:
            import webview
            
            # Start flask in a daemon thread
            t = threading.Thread(target=start_flask, daemon=True)
            t.start()
            
            # Allow Flask to start up
            time.sleep(1)
            
            print("Starting PyWebView Wrapper Mode")
            # Create a native window pointing to the local Flask server
            webview.create_window(
                "DMC Management Suite - Web Edition", 
                "http://127.0.0.1:5000/", 
                width=1200, 
                height=800,
                min_size=(800, 600)
            )
            webview.start()
        except ImportError:
            print("WARNING: pywebview is not installed.")
            print("Falling back to standard Web Server Mode.")
            print("To use the desktop wrapper, please run: pip install pywebview")
            app.run(host="127.0.0.1", port=5000, debug=True)

if __name__ == '__main__':
    main()
