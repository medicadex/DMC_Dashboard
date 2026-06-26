import re

# Update app.py
with open('app.py', 'r', encoding='utf-8') as f:
    app_code = f.read()

# job_form_export
job_form_old = """    data = request.get_json()
    otypes_val = data.get('type', [])
    onames_val = data.get('name', [])
    bus_val = data.get('bu', [])"""

job_form_new = """    if request.is_json:
        data = request.get_json() or {}
        otypes_val = data.get('type', [])
        onames_val = data.get('name', [])
        bus_val = data.get('bu', [])
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
    else:
        otypes_val = request.values.getlist('type')
        onames_val = request.values.getlist('name')
        bus_val = request.values.getlist('bu')
        filters = {
            'bus': bus_val,
            'undertakings': request.values.getlist('undertaking'),
            'otypes': otypes_val,
            'onames': onames_val,
            'feeders': request.values.getlist('feeder'),
            'dts': request.values.getlist('dt'),
            'ftype': request.values.get('form_type', 'Full')
        }
        columns = request.values.getlist('columns')"""

app_code = app_code.replace(job_form_old, job_form_new)

# Remove the old filters and columns assignment below in job_form_export
job_form_filters_old = """    filters = {
        'bus': bus_val,
        'undertakings': data.get('undertaking', []),
        'otypes': otypes_val,
        'onames': onames_val,
        'feeders': data.get('feeder', []),
        'dts': data.get('dt', []),
        'ftype': data.get('form_type', 'Full')
    }
    columns = data.get('columns', [])"""
app_code = app_code.replace(job_form_filters_old, "")


# api_payments_export
payments_old = """    data = request.get_json()
    bus = data.get('bu', [])
    otypes = data.get('type', [])
    onames = data.get('officer', [])
    start_date = data.get('start')
    end_date = data.get('end')"""

payments_new = """    if request.is_json:
        data = request.get_json() or {}
        bus = data.get('bu', [])
        otypes = data.get('type', [])
        onames = data.get('officer', [])
        start_date = data.get('start')
        end_date = data.get('end')
    else:
        bus = request.values.getlist('bu')
        otypes = request.values.getlist('type')
        onames = request.values.getlist('officer')
        start_date = request.values.get('start')
        end_date = request.values.get('end')"""

app_code = app_code.replace(payments_old, payments_new)

# api_customers_export
customers_old = """    data = request.get_json()
    bus = data.get('bu', [])
    otypes = data.get('type', [])
    onames = data.get('officer', [])"""

customers_new = """    if request.is_json:
        data = request.get_json() or {}
        bus = data.get('bu', [])
        otypes = data.get('type', [])
        onames = data.get('officer', [])
    else:
        bus = request.values.getlist('bu')
        otypes = request.values.getlist('type')
        onames = request.values.getlist('officer')"""

app_code = app_code.replace(customers_old, customers_new)

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(app_code)

print("Backend endpoints updated")
