#!/usr/bin/env python3
"""
run_dashboard.py - Single script to start both API and web server
"""

import os
import sys
import time
import threading
import webbrowser
import subprocess
from http.server import HTTPServer, SimpleHTTPRequestHandler

def check_dependencies():
    """Check if required packages are installed"""
    required_packages = ['flask', 'flask_cors', 'snowflake.connector', 'pandas']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package.replace('.', '-'))
    
    if missing_packages:
        print(f"⚠️  Missing packages: {', '.join(missing_packages)}")
        print("Installing required packages...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 
                              'flask', 'flask-cors', 'snowflake-connector-python', 'pandas'])
        print("✅ Dependencies installed successfully!")
        return False
    return True

def start_flask_api():
    """Start the Flask API server"""
    try:
        # Import here after ensuring dependencies are installed
        from app import app
        print("🔧 Starting Flask API on http://localhost:5000")
        app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
    except ImportError:
        print("❌ Error: app.py not found in current directory")
        print("Make sure app.py is in the same folder as this script")
    except Exception as e:
        print(f"❌ Error starting Flask API: {e}")

def start_web_server():
    """Start a simple HTTP server for the HTML dashboard"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    class QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            # Suppress HTTP server logs
            pass
    
    print("🌐 Starting web server on http://localhost:8080")
    httpd = HTTPServer(('localhost', 8080), QuietHTTPRequestHandler)
    httpd.serve_forever()

def check_files():
    """Check if required files exist"""
    required_files = ['app.py', 'index.html']
    missing_files = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing files: {', '.join(missing_files)}")
        print("Please ensure all required files are in the current directory:")
        print("  - app.py (Flask API)")
        print("  - index.html (Dashboard)")
        print("  - private_key.txt (Snowflake key)")
        return False
    
    if not os.path.exists('private_key.txt'):
        print("⚠️  Warning: private_key.txt not found")
        print("   The dashboard will work with sample data only")
        time.sleep(2)
    
    return True

def main():
    """Main function to coordinate startup"""
    print("="*50)
    print("🚀 CQI Dashboard Launcher")
    print("="*50)
    
    # Check files
    if not check_files():
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    # Check and install dependencies
    deps_already_installed = check_dependencies()
    
    if not deps_already_installed:
        print("\n⚠️  Dependencies were just installed.")
        print("Please run this script again to start the dashboard.")
        input("\nPress Enter to exit...")
        sys.exit(0)
    
    print("\n✅ All dependencies are installed")
    
    # Start Flask API in a separate thread
    api_thread = threading.Thread(target=start_flask_api, daemon=True)
    api_thread.start()
    
    # Give Flask time to start
    time.sleep(3)
    
    # Start web server in a separate thread
    web_thread = threading.Thread(target=start_web_server, daemon=True)
    web_thread.start()
    
    # Give web server time to start
    time.sleep(2)
    
    # Open browser
    dashboard_url = "http://localhost:8080/index.html"
    print(f"\n🔵 Opening dashboard in browser: {dashboard_url}")
    webbrowser.open(dashboard_url)
    
    print("\n" + "="*50)
    print("✨ CQI Dashboard is running!")
    print("="*50)
    print(f"📊 Dashboard: {dashboard_url}")
    print(f"🔌 API: http://localhost:5000/api")
    print("\n📝 Troubleshooting:")
    print("  - If the page shows 'Flask API not running', refresh after 5 seconds")
    print("  - Check that port 5000 and 8080 are not in use")
    print("  - Ensure private_key.txt is in the current directory for real data")
    print("\n⛔ Press Ctrl+C to stop all services")
    print("="*50)
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down services...")
        print("Goodbye! 👋")
        sys.exit(0)

if __name__ == "__main__":
    main()