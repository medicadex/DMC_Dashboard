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
    require_password_change = session.pop('require_password_change', False)
    return render_template('home.html', active_page='home', require_password_change=require_password_change)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.get_json()
        username = data.get('username', '').strip()
        password = data.get('password')
        
        if username and username.lower().endswith('@ikejaelectric.com'):
            return jsonify({'success': False, 'message': 'Wrong Username, kindly log in with IE username WITHOUT @ikejaelectric.com'}), 401
            
        if username and username.lower() == 'user':
            return jsonify({'success': False, 'message': 'Kindly sign in with your IE username and staff id'}), 401

        try:
            user_data = auth_service.login(username, password)
            if user_data:
                session['user'] = user_data
                
                # Check for default password
                staff_id = user_data.get('staff_id', '')
                if staff_id and str(password).strip() == str(staff_id).strip().lower():
                    session['require_password_change'] = True
                    
                return jsonify({'success': True, 'user': user_data})
            
            attempts = session_manager.login_attempts.get(username, 0)
            if attempts >= 3:
                return jsonify({'success': False, 'requires_reset': True, 'username': username, 'message': 'Maximum login attempts reached.'}), 401
                
            return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
        except Exception as e:
            logger.error(f"Login error: {e}", exc_info=True)
            return jsonify({'success': False, 'message': 'An error occurred during authentication.'}), 401
    return render_template('login.html')

@app.route('/api/staff/reset-password-forced', methods=['POST'])
def reset_password_forced():
    data = request.get_json()
    username = data.get('username')
    staff_id = data.get('staff_id')
    new_password = data.get('new_password')

    if not all([username, staff_id, new_password]):
        return jsonify({'success': False, 'message': 'All fields are required.'}), 400

    success, message = auth_service.reset_password_via_staff_id(username, staff_id, new_password)
    if success:
        return jsonify({'success': True, 'message': message})
    return jsonify({'success': False, 'message': message}), 400

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
    

import csv
from werkzeug.utils import secure_filename
from utils.security import SecurityManager

@app.route('/api/staff/request-profile', methods=['POST'])
def request_profile():
    data = request.get_json()
    action = data.get('action_type', 'CREATE')
    if action == 'CREATE':
        if data.get('password'):
            data['password_hash'] = SecurityManager.hash_password(data['password'])
        else:
            return jsonify({'success': False, 'message': 'Password required for new profile.'}), 400
    else:
        if data.get('password'):
            data['password_hash'] = SecurityManager.hash_password(data['password'])
        
    submitted_by = session['user']['username'] if 'user' in session else data.get('username')
    success = staff_repo.create_pending_profile(action, data, submitted_by)
    if success:
        return jsonify({'success': True, 'message': 'Profile request submitted for admin approval.'})
    else:
        return jsonify({'success': False, 'message': 'Failed to submit profile request.'}), 500

@app.route('/admin/staff')
@login_required
def admin_staff_management():
    if session['user'].get('role') != 'Admin':
        return redirect(url_for('index'))
    return render_template('admin_staff_management.html', active_page='admin_staff')

@app.route('/api/staff/pending', methods=['GET'])
@login_required
def get_pending_profiles():
    if session['user'].get('role') != 'Admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    profiles = staff_repo.get_pending_profiles()
    return jsonify({'success': True, 'data': profiles})

@app.route('/api/staff/approve/<int:req_id>', methods=['POST'])
@login_required
def approve_pending_profile(req_id):
    if session['user'].get('role') != 'Admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    req = staff_repo.get_pending_profile_by_id(req_id)
    if not req:
        return jsonify({'success': False, 'message': 'Request not found'}), 404
        
    if req.get('action_type') == 'CREATE':
        em = req.get('email', '')
        calc_username = em.split('@')[0] if em else None
        staff_repo.add_staff(
            username=calc_username,
            hashed_pwd=req.get('password_hash') or '',
            first_name=req.get('first_name', ''),
            surname=req.get('surname', ''),
            role=req.get('role', 'User'),
            email=req.get('email'),
            phone_number=req.get('phone_number'),
            staff_id=req.get('staff_id'),
            officer_type=req.get('officer_type'),
            business_unit=req.get('business_unit'),
            name_official=req.get('name_official'),
            name_variant=req.get('name_variant')
        )
    elif req.get('action_type') == 'UPDATE':
        updates = []
        params = {"sid": req.get('staff_id')}
        for field in ['first_name', 'surname', 'name_official', 'name_variant', 'officer_type', 'business_unit', 'email', 'phone_number']:
            if req.get(field):
                updates.append(f"{field} = :{field}")
                params[field] = req.get(field)
                
        em = req.get('email', '')
        if em:
            updates.append("username = :uname")
            params['uname'] = em.split('@')[0]

        if updates and params['sid']:
            with engine.begin() as conn:
                conn.execute(text(f"UPDATE staff SET {', '.join(updates)} WHERE staff_id = :sid"), params)
                
    staff_repo.update_pending_profile_status(req_id, 'APPROVED')
    return jsonify({'success': True, 'message': 'Profile request approved.'})

@app.route('/api/staff/reject/<int:req_id>', methods=['POST'])
@login_required
def reject_pending_profile(req_id):
    if session['user'].get('role') != 'Admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    staff_repo.update_pending_profile_status(req_id, 'REJECTED')
    return jsonify({'success': True, 'message': 'Profile request rejected.'})

@app.route('/admin/staff/template')
@login_required
def download_staff_template():
    if session['user'].get('role') != 'Admin':
        return redirect(url_for('index'))
    csv_data = "staff_id,username,first_name,surname,name_official,name_variant,email,phone_number,officer_type,business_unit,role\n"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=staff_upload_template.csv"}
    )

@app.route('/api/admin/staff/create', methods=['POST'])
@login_required
def admin_create_staff():
    if session['user'].get('role') != 'Admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    data = request.get_json()
    sid = str(data.get('staff_id', '')).strip()
    uname = str(data.get('username', '')).strip()
    if not uname or not sid:
        return jsonify({'success': False, 'message': 'Username and Staff ID are required.'}), 400
        
    fname = str(data.get('first_name', '')).strip()
    sname = str(data.get('surname', '')).strip()
    no = f"{fname} {sname}".strip()
    em = str(data.get('email', '')).strip()
    ph = str(data.get('phone_number', '')).strip()
    ot = str(data.get('officer_type', '')).strip()
    bu = str(data.get('business_unit', '')).strip()
    r = str(data.get('role', 'User')).strip()
    
    # Default password is the staff ID
    phash = SecurityManager.hash_password(sid)
    
    try:
        with engine.begin() as conn:
            query = text("""
                INSERT INTO staff (staff_id, username, first_name, surname, name_official, email, phone_number, officer_type, business_unit, role, password_hash, sync_status)
                VALUES (:sid, :uname, :fname, :sname, :no, :em, :ph, :ot, :bu, :r, :phash, 'SYNCED')
                ON DUPLICATE KEY UPDATE
                first_name=VALUES(first_name), surname=VALUES(surname), name_official=VALUES(name_official),
                email=VALUES(email), phone_number=VALUES(phone_number), officer_type=VALUES(officer_type), 
                business_unit=VALUES(business_unit), role=VALUES(role), password_hash=VALUES(password_hash)
            """)
            conn.execute(query, {
                'sid': sid, 'uname': uname, 'fname': fname, 'sname': sname, 'no': no,
                'em': em, 'ph': ph, 'ot': ot, 'bu': bu, 'r': r, 'phash': phash
            })
        return jsonify({'success': True, 'message': 'Staff registered successfully.'})
    except Exception as e:
        logger.error(f"Failed to create single staff record: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'Error creating staff: {str(e)}'}), 500

@app.route('/api/admin/staff/all', methods=['GET'])
@login_required
def admin_get_all_staff():
    if session['user'].get('role') != 'Admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    staff_list = staff_repo.get_all_staff_detailed()
    return jsonify({'success': True, 'data': staff_list})

@app.route('/api/admin/staff/update/<int:user_id>', methods=['POST'])
@login_required
def admin_update_staff(user_id):
    if session['user'].get('role') != 'Admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    data = request.get_json()
    sid = str(data.get('staff_id', '')).strip()
    if not sid:
        return jsonify({'success': False, 'message': 'Staff ID is required.'}), 400
        
    fname = str(data.get('first_name', '')).strip()
    sname = str(data.get('surname', '')).strip()
    no = f"{fname} {sname}".strip()
    em = str(data.get('email', '')).strip()
    ph = str(data.get('phone_number', '')).strip()
    ot = str(data.get('officer_type', '')).strip()
    bu = str(data.get('business_unit', '')).strip()
    r = str(data.get('role', 'User')).strip()
    
    uname = em.split('@')[0] if em else None
    
    updates = []
    params = {'uid': user_id, 'sid': sid, 'uname': uname, 'fname': fname, 'sname': sname, 'no': no, 'em': em, 'ph': ph, 'ot': ot, 'bu': bu, 'r': r}
    
    # Check if we should update password
    new_pwd = data.get('password')
    if new_pwd:
        params['phash'] = SecurityManager.hash_password(new_pwd)
        updates.append("password_hash = :phash")
        
    try:
        with engine.begin() as conn:
            query = text(f"""
                UPDATE staff SET 
                staff_id = :sid, username = :uname, first_name = :fname, surname = :sname, 
                name_official = :no, email = :em, phone_number = :ph, 
                officer_type = :ot, business_unit = :bu, role = :r
                {", " + ", ".join(updates) if updates else ""}
                WHERE id = :uid
            """)
            conn.execute(query, params)
        return jsonify({'success': True, 'message': 'Staff updated successfully.'})
    except Exception as e:
        logger.error(f"Failed to update staff record: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'Error updating staff: {str(e)}'}), 500

@app.route('/api/staff/bulk-upload', methods=['POST'])
@login_required
def bulk_upload_staff():
    if session['user'].get('role') != 'Admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No selected file'}), 400
        
    if file:
        import pandas as pd
        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
                
            df = df.fillna('')
            count = 0
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    sid = str(row.get('staff_id', '')).strip()
                    em = str(row.get('email', '')).strip()
                    uname = em.split('@')[0] if em else None
                    
                    if not sid: continue
                    
                    fname = str(row.get('first_name', '')).strip()
                    sname = str(row.get('surname', '')).strip()
                    no = str(row.get('name_official', '')).strip()
                    nv = str(row.get('name_variant', '')).strip()
                    ph = str(row.get('phone_number', '')).strip()
                    ot = str(row.get('officer_type', '')).strip()
                    bu = str(row.get('business_unit', '')).strip()
                    r = str(row.get('role', 'User')).strip()
                    
                    default_pwd = sid if sid else uname
                    phash = SecurityManager.hash_password(default_pwd)
                    
                    query = text("""
                        INSERT INTO staff (staff_id, username, first_name, surname, name_official, name_variant, email, phone_number, officer_type, business_unit, role, password_hash, sync_status)
                        VALUES (:sid, :uname, :fname, :sname, :no, :nv, :em, :ph, :ot, :bu, :r, :phash, 'SYNCED')
                        ON DUPLICATE KEY UPDATE
                        username=IF(VALUES(username) IS NOT NULL, VALUES(username), username),
                        first_name=IF(VALUES(first_name) != '', VALUES(first_name), first_name),
                        surname=IF(VALUES(surname) != '', VALUES(surname), surname),
                        name_official=IF(VALUES(name_official) != '', VALUES(name_official), name_official),
                        name_variant=IF(VALUES(name_variant) != '', VALUES(name_variant), name_variant),
                        email=IF(VALUES(email) != '', VALUES(email), email),
                        phone_number=IF(VALUES(phone_number) != '', VALUES(phone_number), phone_number),
                        officer_type=IF(VALUES(officer_type) != '', VALUES(officer_type), officer_type),
                        business_unit=IF(VALUES(business_unit) != '', VALUES(business_unit), business_unit),
                        role=IF(VALUES(role) != '', VALUES(role), role)
                    """)
                    conn.execute(query, {
                        'sid': sid, 'uname': uname, 'fname': fname, 'sname': sname, 'no': no, 'nv': nv,
                        'em': em, 'ph': ph, 'ot': ot, 'bu': bu, 'r': r, 'phash': phash
                    })
                    count += 1
            return jsonify({'success': True, 'message': f'Successfully uploaded {count} staff records.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error processing file: {str(e)}'}), 500

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

from reportlab.pdfgen import canvas as pdf_canvas

class WatermarkCanvas(pdf_canvas.Canvas):
    def showPage(self):
        from reportlab.lib import colors
        self.saveState()
        self.setFont('Helvetica-Bold', 60)
        self.setFillColor(colors.HexColor('#CCCCCC'))
        try:
            self.setFillAlpha(0.35)
        except AttributeError:
            pass
        self.translate(306, 396)
        self.rotate(45)
        self.drawCentredString(0, 0, "SAMPLE")
        self.restoreState()
        super().showPage()

@app.route('/api/account/soa/<account_number>')
@login_required
def api_account_soa(account_number):
    try:
        # Enforce Vendor role data isolation at the lookup boundary
        if session['user']['role'] == 'Vendor':
            sql = text("SELECT account_officer FROM customers WHERE account_number = :acc")
            with engine.connect() as conn:
                row = conn.execute(sql, {"acc": account_number}).fetchone()
            if not row or row[0] != session['user']['username']:
                return jsonify({"error": "Access denied. This account is not assigned to your agency."}), 403

        # Fetch demographics and financials using AccountService
        data = account_service.get_account_financials(account_number, session['user']['username'], session['user']['role'], force_online=True)
        if not data:
            return jsonify({"error": "Account not found"}), 404

        acc = data['account']
        fin = data['financials']

        # Query database for detailed discount and adjustment entries with dates
        with engine.connect() as conn:
            discounts_raw = conn.execute(text("""
                SELECT discounted_amount, date_applied, date_approved, user_who_approved, status 
                FROM discounts WHERE account_number = :acc
            """), {"acc": account_number}).fetchall()
            
            adjustments_raw = conn.execute(text("""
                SELECT adjustment_amount, date_applied, date_approved, user_who_approved_adjustment, status, remark 
                FROM adjustments WHERE account_number = :acc
            """), {"acc": account_number}).fetchall()
            
            # Fetch payments details
            payments_raw = conn.execute(text("""
                SELECT date_of_payment, amount_paid, payment_source 
                FROM all_payments WHERE account_number = :acc
            """), {"acc": account_number}).fetchall()

        def normalize_date(d):
            if d is None:
                return datetime(2000, 1, 1)
            if isinstance(d, datetime):
                return d
            if type(d).__name__ == 'date':
                return datetime(d.year, d.month, d.day)
            if type(d).__name__ == 'Timestamp':
                return d.to_pydatetime()
            if isinstance(d, str):
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d-%m-%Y %H:%M:%S', '%d-%m-%Y'):
                    try:
                        return datetime.strptime(d.strip()[:19], fmt)
                    except:
                        pass
            return datetime(2000, 1, 1)

        # Build chronological ledger
        setup_dt = normalize_date(acc.get('setup_date'))
        initial_debt_row = {
            "date": setup_dt,
            "description": "Initial Debt (at Deactivation)",
            "details": "--",
            "amount": float(acc.get('closing_balance') or 0.0),
            "status": "Approved",
            "impact": float(acc.get('closing_balance') or 0.0)
        }

        transactions = []

        # Process payments
        for p in payments_raw:
            dop = normalize_date(p[0])
            transactions.append({
                "date": dop,
                "description": "Payments" if str(p[2]).lower() == 'collection' else f"Payment ({p[2]})",
                "details": f"Source: {p[2]}",
                "amount": float(p[1] or 0.0),
                "status": "Approved",
                "impact": -float(p[1] or 0.0)
            })

        # Process discounts
        for d in discounts_raw:
            amt = float(d[0] or 0.0)
            date_app = normalize_date(d[2] or d[1])
            approver = str(d[3]).lower() if d[3] else ""
            status = str(d[4]).lower() if d[4] else ""
            val_status = validation_service.validate_transaction('discount', amt, approver, status)
            
            desc = "Discount (Approved)" if val_status == 'valid' else ("Discount (Rejected)" if val_status == 'rejected' else "Discount (Pending)")
            impact = -amt if val_status == 'valid' else 0.0
            status_label = "Approved" if val_status == 'valid' else ("Rejected" if val_status == 'rejected' else "Pending")
            
            transactions.append({
                "date": date_app,
                "description": desc,
                "details": f"Approver: {d[3] or '--'}",
                "amount": amt,
                "status": status_label,
                "impact": impact
            })

        # Process adjustments
        for a in adjustments_raw:
            amt = float(a[0] or 0.0)
            date_app = normalize_date(a[2] or a[1])
            approver = str(a[3]).lower() if a[3] else ""
            status = str(a[4]).lower() if a[4] else ""
            remark = str(a[5]) if a[5] else ""
            val_status = validation_service.validate_transaction('adjustment', amt, approver, status)
            
            desc = "Adjustment (Approved)" if val_status == 'valid' else ("Adjustment (Rejected)" if val_status == 'rejected' else "Adjustment (Pending)")
            impact = -amt if val_status == 'valid' else 0.0
            status_label = "Approved" if val_status == 'valid' else ("Rejected" if val_status == 'rejected' else "Pending")
            
            transactions.append({
                "date": date_app,
                "description": desc,
                "details": f"Remark: {remark}" if remark else f"Approver: {a[3] or '--'}",
                "amount": amt,
                "status": status_label,
                "impact": impact
            })

        # Sort chronological transactions
        transactions.sort(key=lambda t: t['date'])
        ledger = [initial_debt_row] + transactions

        # Compute running balance
        r_bal = 0.0
        for entry in ledger:
            r_bal += entry['impact']
            entry['running_balance'] = r_bal

        # Build PDF using ReportLab
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib import colors

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36)
        story = []

        styles = getSampleStyleSheet()
        
        # Register Arial fonts if available (supports Naira ₦ character on Windows)
        font_name = 'Helvetica'
        font_bold_name = 'Helvetica-Bold'
        font_italic_name = 'Helvetica-Oblique'
        
        try:
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont
            import os
            arial_path = r"C:\Windows\Fonts\arial.ttf"
            arial_bold_path = r"C:\Windows\Fonts\arialbd.ttf"
            arial_italic_path = r"C:\Windows\Fonts\ariali.ttf"
            if os.path.exists(arial_path) and os.path.exists(arial_bold_path):
                pdfmetrics.registerFont(TTFont('Arial', arial_path))
                pdfmetrics.registerFont(TTFont('Arial-Bold', arial_bold_path))
                font_name = 'Arial'
                font_bold_name = 'Arial-Bold'
                if os.path.exists(arial_italic_path):
                    pdfmetrics.registerFont(TTFont('Arial-Italic', arial_italic_path))
                    font_italic_name = 'Arial-Italic'
                else:
                    font_italic_name = 'Arial'
        except Exception as e:
            logger.warning(f"Failed to register Arial fonts: {e}")
            
        curr = '₦' if font_name == 'Arial' else 'N'
        
        # Color codes
        ie_pink = colors.HexColor('#E00C7E')
        text_dark = colors.HexColor('#1E293B')
        text_muted = colors.HexColor('#64748B')
        success_green = colors.HexColor('#10B981')
        danger_red = colors.HexColor('#EF4444')

        title_style = ParagraphStyle(
            'TitleStyle',
            parent=styles['Heading1'],
            fontName=font_bold_name,
            fontSize=20,
            textColor=ie_pink,
            spaceAfter=2
        )
        subtitle_style = ParagraphStyle(
            'SubtitleStyle',
            parent=styles['Normal'],
            fontName=font_name,
            fontSize=9,
            textColor=text_muted,
            spaceAfter=10
        )
        label_style = ParagraphStyle(
            'LabelStyle',
            parent=styles['Normal'],
            fontName=font_bold_name,
            fontSize=9,
            textColor=text_muted
        )
        value_style = ParagraphStyle(
            'ValueStyle',
            parent=styles['Normal'],
            fontName=font_name,
            fontSize=9,
            textColor=text_dark
        )
        th_style = ParagraphStyle(
            'THStyle',
            parent=styles['Normal'],
            fontName=font_bold_name,
            fontSize=9,
            textColor=colors.white
        )
        td_style = ParagraphStyle(
            'TDStyle',
            parent=styles['Normal'],
            fontName=font_name,
            fontSize=8,
            textColor=text_dark
        )
        td_bold_style = ParagraphStyle(
            'TDBoldStyle',
            parent=styles['Normal'],
            fontName=font_bold_name,
            fontSize=8,
            textColor=text_dark
        )
        card_title_style = ParagraphStyle(
            'CardTitle',
            parent=styles['Normal'],
            fontName=font_bold_name,
            fontSize=9,
            textColor=text_muted,
            alignment=1
        )
        card_val_style = ParagraphStyle(
            'CardVal',
            parent=styles['Normal'],
            fontName=font_bold_name,
            fontSize=12,
            textColor=text_dark,
            alignment=1
        )

        # 1. Header (Logo + Title)
        logo_path = os.path.join(app.root_path, 'static', 'ie_logo.png')
        header_data = []
        if os.path.exists(logo_path):
            img = Image(logo_path, width=50, height=40)
            header_data.append([img, [
                Paragraph("IKEJA ELECTRIC", title_style),
                Paragraph("<b>STATEMENT OF ACCOUNT</b>", ParagraphStyle('SubTitle', fontName=font_bold_name, fontSize=12, textColor=text_dark)),
                Paragraph("Debt Management and Control Department", subtitle_style)
            ]])
        else:
            header_data.append(["", [
                Paragraph("IKEJA ELECTRIC", title_style),
                Paragraph("<b>STATEMENT OF ACCOUNT</b>", ParagraphStyle('SubTitle', fontName=font_bold_name, fontSize=12, textColor=text_dark)),
                Paragraph("Debt Management and Control Department", subtitle_style)
            ]])
        
        header_table = Table(header_data, colWidths=[70, 470])
        header_table.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ]))
        story.append(header_table)

        # 2. Separator line
        sep_table = Table([[""]], colWidths=[540])
        sep_table.setStyle(TableStyle([
            ('LINEBELOW', (0,0), (-1,-1), 2, ie_pink),
            ('BOTTOMPADDING', (0,0), (-1,-1), 15),
        ]))
        story.append(sep_table)
        story.append(Spacer(1, 10))

        # 3. Demographics block
        dem_data = [
            [Paragraph("<b>Customer Name:</b>", label_style), Paragraph(str(acc.get('account_name') or '--').upper(), value_style), Paragraph("<b>Account Number:</b>", label_style), Paragraph(str(acc.get('account_number') or '--'), value_style)],
            [Paragraph("<b>Service Address:</b>", label_style), Paragraph(str(acc.get('account_address') or '--'), value_style), Paragraph("<b>Business Unit:</b>", label_style), Paragraph(str(acc.get('business_unit') or '--'), value_style)],
            [Paragraph("<b>Undertaking:</b>", label_style), Paragraph(str(acc.get('undertaking') or '--'), value_style), Paragraph("<b>Account Officer:</b>", label_style), Paragraph(str(acc.get('account_officer') or '--'), value_style)],
            [Paragraph("<b>DT Name:</b>", label_style), Paragraph(str(acc.get('dt_name') or '--'), value_style), Paragraph("<b>Feeder Band:</b>", label_style), Paragraph(str(acc.get('feeder') or '--'), value_style)]
        ]
        
        dem_table = Table(dem_data, colWidths=[100, 170, 100, 170])
        dem_table.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#F8FAFC')),
            ('PADDING', (0,0), (-1,-1), 6),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#E2E8F0')),
        ]))
        story.append(dem_table)
        story.append(Spacer(1, 15))

        # 4. Financial Summary Grid
        pp_color = '#10B981' if fin.get('payment_plan_status') == 'Active' else ('#EF4444' if fin.get('payment_plan_status') == 'Defaulted' else '#64748B')
        
        summary_cards_data = [
            [
                Paragraph("Total Debt", card_title_style),
                Paragraph("Total Payments", card_title_style),
                Paragraph("Outstanding Balance", card_title_style),
                Paragraph("Payment Plan / Status", card_title_style)
            ],
            [
                Paragraph(f"{curr}{float(acc.get('closing_balance') or 0):,.2f}", card_val_style),
                Paragraph(f"{curr}{float(fin.get('total_payments') or 0):,.2f}", card_val_style),
                Paragraph(f"<font color='#EF4444'>{curr}{float(fin.get('outstanding_balance') or 0):,.2f}</font>", card_val_style),
                Paragraph(f"{fin.get('payment_plan', 'No')} / <font color='{pp_color}'><b>{str(fin.get('payment_plan_status', 'No Plan')).upper()}</b></font>", card_val_style)
            ]
        ]
        
        summary_table = Table(summary_cards_data, colWidths=[135, 135, 135, 135])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#F1F5F9')),
            ('BACKGROUND', (0,1), (-1,1), colors.HexColor('#F8FAFC')),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('GRID', (0,0), (-1,-1), 1, colors.HexColor('#CBD5E1')),
            ('PADDING', (0,0), (-1,-1), 8),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 20))

        # 5. Ledger Title
        story.append(Paragraph("<b>TRANSACTION HISTORY LEDGER</b>", ParagraphStyle('TableTitle', fontName=font_bold_name, fontSize=10, textColor=ie_pink, spaceAfter=8)))

        # 6. Ledger Table
        table_headers = [
            Paragraph("Date", th_style),
            Paragraph("Description", th_style),
            Paragraph(f"Amount ({curr})", th_style),
            Paragraph("Status", th_style),
            Paragraph(f"Outstanding ({curr})", th_style)
        ]
        
        ledger_table_data = [table_headers]
        
        for entry in ledger:
            date_str = entry['date'].strftime('%Y-%m-%d') if isinstance(entry['date'], datetime) else str(entry['date'])
            amt_str = f"{curr}{entry['amount']:,.2f}"
            bal_str = f"{curr}{entry['running_balance']:,.2f}"
            
            status_style = td_bold_style if entry['status'] == 'Approved' else ParagraphStyle('StatusStyle', parent=td_bold_style, textColor=danger_red if entry['status'] == 'Rejected' else colors.HexColor('#F59E0B'))
            
            impact_sign = "-" if entry['impact'] < 0 else ("+" if entry['impact'] > 0 else "")
            formatted_amt = f"{impact_sign} {amt_str}" if impact_sign else amt_str
            
            row = [
                Paragraph(date_str, td_style),
                Paragraph(entry['description'], td_bold_style),
                Paragraph(formatted_amt, td_bold_style),
                Paragraph(entry['status'], status_style),
                Paragraph(bal_str, td_bold_style)
            ]
            ledger_table_data.append(row)

        ledger_table = Table(ledger_table_data, colWidths=[80, 180, 100, 80, 100])
        
        t_style = [
            ('BACKGROUND', (0,0), (-1,0), ie_pink),
            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('BOTTOMPADDING', (0,0), (-1,0), 6),
            ('TOPPADDING', (0,0), (-1,0), 6),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#E2E8F0')),
            ('PADDING', (0,1), (-1,-1), 6),
        ]
        
        for i in range(1, len(ledger_table_data)):
            if i % 2 == 0:
                t_style.append(('BACKGROUND', (0,i), (-1,i), colors.HexColor('#F8FAFC')))
                
        ledger_table.setStyle(TableStyle(t_style))
        story.append(ledger_table)
        story.append(Spacer(1, 15))

        warning_bold_style = ParagraphStyle(
            'WarningBold',
            parent=styles['Normal'],
            fontName=font_bold_name,
            fontSize=10,
            textColor=danger_red,
            alignment=1,
            spaceAfter=15
        )
        story.append(Paragraph("<b>NOT FOR CUSTOMERS, FOR OFFICIAL USE ONLY</b>", warning_bold_style))
        story.append(Spacer(1, 10))

        # 7. Footnotes and Disclaimers
        footer_style = ParagraphStyle(
            'FooterNotes',
            parent=styles['Normal'],
            fontName=font_italic_name,
            fontSize=7,
            textColor=text_muted,
            alignment=1
        )
        story.append(Paragraph("This is an official document generated by the Ikeja Electric Debt Management & Recovery Portal.<br/>Only approved adjustments and discounts are applied towards the outstanding balance calculation.", footer_style))

        # Build document with watermark on top
        doc.build(story, canvasmaker=WatermarkCanvas)
        buffer.seek(0)
        
        # Log activity
        staff_repo.log_activity(session['user']['username'], "GENERATE_SOA_PDF", f"Generated statement for account {account_number}", event_type='MINOR')

        return send_file(
            buffer,
            as_attachment=False,
            download_name=f"SOA_{account_number}.pdf",
            mimetype="application/pdf"
        )
    except Exception as e:
        logger.error(f"Error generating statement of account PDF for {account_number}: {e}", exc_info=True)
        return jsonify({"error": "An error occurred while generating the PDF statement of account."}), 500

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
            
        df['account_officer'] = df['account_officer'].fillna('Unknown')
        df['business_unit'] = df['business_unit'].fillna('Unknown')
        df['officer_type'] = df['officer_type'].fillna('Unknown')
        df['recovery'] = df['recovery'].fillna(0.0)

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
