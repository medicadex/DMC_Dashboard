import os

with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("os.getenv('SECRET_KEY', os.urandom(24))", "os.getenv('SECRET_KEY', 'default-static-secret-key-for-dmc-web-project-12345')")

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)
