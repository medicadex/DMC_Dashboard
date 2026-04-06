import sys
import threading
import time
from app import app
import os

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
