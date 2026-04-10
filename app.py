from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory
from db_utils import get_db_engine
from datetime import datetime
from sqlalchemy import text
import os

from services.account_service import AccountService
from services.validation_service import ValidationService
from repositories.staff_repo import StaffRepository

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
engine = get_db_engine()
staff_repo = StaffRepository(engine)
validation_service = ValidationService(engine)
account_service = AccountService(engine, staff_repo, validation_service)

@app.route('/')
def index():
    return redirect(url_for('dashboard'))

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'ie_logo.png', mimetype='image/png')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/manifest.json')
def serve_manifest():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'manifest.json')

@app.route('/service-worker.js')
def serve_sw():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'service-worker.js')

@app.route('/api/autocomplete')
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
def api_account_dashboard(account_number):
    try:
        # In the web version, we don't strict-enforce the role-based limitations from staff session yet 
        # unless implemented, so we pass 'System' for username and 'Admin' for role.
        # We also pass force_online=True to guarantee it reliably queries the AWS RDS instead of falling back
        # to local SQLite (since local might not have full validation sync).
        data = account_service.get_account_financials(account_number, 'System', 'Admin', force_online=True)
        
        if not data:
            return jsonify({"error": "Account not found"}), 404
            
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
