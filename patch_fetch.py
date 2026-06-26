import re
import os

# 1. Patch app.py
with open('app.py', 'r', encoding='utf-8') as f:
    app_code = f.read()

# Change request.args to request.values globally in these routes
# We can just replace request.args with request.values everywhere since they behave the same for query params
app_code = app_code.replace("request.args.getlist", "request.values.getlist")
app_code = app_code.replace("request.args.get", "request.values.get")

# Add POST method to the specific endpoints
endpoints = [
    '/api/payments/preview',
    '/api/job-form/undertakings',
    '/api/job-form/officers',
    '/api/job-form/feeders',
    '/api/job-form/dts',
    '/api/job-form/preview',
    '/api/job-form/count'
]

for ep in endpoints:
    # Look for @app.route('endpoint') or @app.route('endpoint', methods=['GET'])
    # Replace with @app.route('endpoint', methods=['GET', 'POST'])
    pattern = r"@app\.route\('" + ep + r"'\)"
    app_code = re.sub(pattern, f"@app.route('{ep}', methods=['GET', 'POST'])", app_code)

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(app_code)

# 2. Patch HTML files
def patch_html(filepath):
    if not os.path.exists(filepath): return
    with open(filepath, 'r', encoding='utf-8') as f:
        html = f.read()
    
    # We want to replace things like:
    # await fetch(`/api/job-form/dts?${query.toString()}`);
    # with:
    # await fetch(`/api/job-form/dts`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: query.toString() });
    
    # Regex to find `fetch(\`...?\${query.toString()}\`)`
    pattern = r"fetch\(`([^`?]+)\?\$\{query\.toString\(\)\}`\)"
    replacement = r"fetch(`\1`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: query.toString() })"
    
    html = re.sub(pattern, replacement, html)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)

patch_html('templates/job_form.html')
patch_html('templates/payments.html')

print("Patch applied successfully.")
