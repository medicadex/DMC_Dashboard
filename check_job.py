import sys
with open('templates/job_form.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if '/api/job-form/export' in line:
        for j in range(max(0, i-25), min(len(lines), i+35)):
            print(f'{j+1}: {lines[j].encode("utf-8", "ignore").decode("utf-8").rstrip()}')
        break
