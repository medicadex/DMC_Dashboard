import re

def replace_export_logic(filepath, action_url, inputs_logic, form_name='form'):
    with open(filepath, 'r', encoding='utf-8') as f:
        html = f.read()
        
    # Find the start of the try block in the export function
    if 'job_form.html' in filepath:
        match = re.search(r'try\s*\{\s*const response = await fetch\(\'/api/job-form/export\'.*?catch.*?\}', html, re.DOTALL)
    elif 'payments.html' in filepath:
        match = re.search(r'try\s*\{\s*const response = await fetch\(\'/api/payments/export\'.*?catch.*?\}', html, re.DOTALL)
    elif 'customers.html' in filepath:
        match = re.search(r'try\s*\{\s*const response = await fetch\(\'/api/customers/export\'.*?catch.*?\}', html, re.DOTALL)
    elif 'performance_full.html' in filepath:
        match = re.search(r'try\s*\{\s*const response = await fetch\(\'/api/performance/export\'.*?finally\s*\{.*?\}', html, re.DOTALL)
        if not match:
            match = re.search(r'try\s*\{\s*const response = await fetch\(\'/api/performance/export\'.*?catch.*?\}', html, re.DOTALL)

    if not match:
        print(f"Could not find fetch block in {filepath}")
        return

    replacement = f"""try {{
            const form = document.createElement('form');
            form.method = 'POST';
            form.action = '{action_url}';
            form.target = '_blank';
            form.style.display = 'none';

            const addInput = (name, value) => {{
                if (value !== null && value !== undefined) {{
                    const input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = name;
                    input.value = value;
                    form.appendChild(input);
                }}
            }};

{inputs_logic}

            document.body.appendChild(form);
            form.submit();
            
            setTimeout(() => {{
                document.body.removeChild(form);
                if (typeof btn !== 'undefined' && btn && typeof originalHtml !== 'undefined') {{
                    btn.disabled = false;
                    btn.innerHTML = originalHtml;
                }}
            }}, 1500);
        }} catch (err) {{
            console.error("Export error:", err);
            alert("An error occurred during file export.");
        }}"""
    
    html = html[:match.start()] + replacement + html[match.end():]
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Updated {filepath}")

# Job Form
inputs_job_form = """            bus.forEach(b => addInput('bu', b));
            undertakings.forEach(u => addInput('undertaking', u));
            types.forEach(t => addInput('type', t));
            names.forEach(n => addInput('name', n));
            feeders.forEach(f => addInput('feeder', f));
            dts.forEach(d => addInput('dt', d));
            columns.forEach(c => addInput('columns', c));
            addInput('form_type', formType);"""
replace_export_logic('templates/job_form.html', '/api/job-form/export', inputs_job_form)

# Payments
inputs_payments = """            bus.forEach(b => addInput('bu', b));
            types.forEach(t => addInput('type', t));
            officers.forEach(o => addInput('officer', o));
            addInput('start', start);
            addInput('end', end);"""
replace_export_logic('templates/payments.html', '/api/payments/export', inputs_payments)

# Customers
inputs_customers = """            bus.forEach(b => addInput('bu', b));
            types.forEach(t => addInput('type', t));
            officers.forEach(o => addInput('officer', o));"""
replace_export_logic('templates/customers.html', '/api/customers/export', inputs_customers)

# Performance Full
inputs_performance = """            addInput('period', urlParams.get('period'));
            addInput('otype', urlParams.get('otype'));
            addInput('start', urlParams.get('start'));
            addInput('end', urlParams.get('end'));
            addInput('year', urlParams.get('year'));
            addInput('quarter', urlParams.get('quarter'));"""
replace_export_logic('templates/performance_full.html', '/api/performance/export', inputs_performance)
