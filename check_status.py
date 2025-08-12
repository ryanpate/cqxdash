#!/usr/bin/env python3
"""
check_status.py - Verify your CQI Dashboard setup and connections
"""

import sys
import json
import time
from datetime import datetime
import requests
from colorama import init, Fore, Style

# Initialize colorama for Windows color support
init(autoreset=True)

def print_header():
    """Print a nice header"""
    print("\n" + "="*60)
    print(Fore.CYAN + "      CQI DASHBOARD STATUS CHECKER")
    print("="*60 + "\n")

def check_api_health():
    """Check if Flask API is running"""
    print(Fore.YELLOW + "1. Checking Flask API..." + Style.RESET_ALL)
    try:
        response = requests.get("http://localhost:5000/api/health", timeout=3)
        if response.status_code == 200:
            print(Fore.GREEN + "   ✅ API is running and healthy")
            return True
        else:
            print(Fore.RED + "   ❌ API returned error status")
            return False
    except requests.exceptions.ConnectionError:
        print(Fore.RED + "   ❌ API is not running (Connection refused)")
        print("   Run: python app.py")
        return False
    except Exception as e:
        print(Fore.RED + f"   ❌ Error: {e}")
        return False

def check_snowflake_connection():
    """Check if Snowflake connection works"""
    print(Fore.YELLOW + "\n2. Checking Snowflake Connection..." + Style.RESET_ALL)
    try:
        response = requests.get("http://localhost:5000/api/filters", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'error' in data:
                print(Fore.RED + f"   ❌ Snowflake error: {data['error']}")
                return False
            else:
                print(Fore.GREEN + "   ✅ Snowflake connection successful")
                if 'submarkets' in data:
                    print(f"   📊 Found {len(data['submarkets'])} submarkets")
                if 'cqeClusters' in data:
                    print(f"   📊 Found {len(data['cqeClusters'])} CQE clusters")
                if 'metricNames' in data:
                    print(f"   📊 Found {len(data['metricNames'])} metrics")
                return True
        else:
            print(Fore.RED + "   ❌ Failed to fetch filter options")
            return False
    except Exception as e:
        print(Fore.RED + f"   ❌ Error: {e}")
        return False

def check_data_retrieval():
    """Check if data can be retrieved"""
    print(Fore.YELLOW + "\n3. Checking Data Retrieval..." + Style.RESET_ALL)
    try:
        # Get data for last 2 days
        params = {
            'periodStart': '2025-08-10',
            'periodEnd': '2025-08-12'
        }
        response = requests.get("http://localhost:5000/api/data", params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                print(Fore.GREEN + f"   ✅ Successfully retrieved {len(data)} records")
                if len(data) > 0:
                    # Show sample record structure
                    print("   📋 Sample record fields:")
                    sample = data[0]
                    for key in list(sample.keys())[:5]:
                        print(f"      - {key}")
                return True
            else:
                print(Fore.YELLOW + "   ⚠️  No data returned (might be empty result)")
                return True
        else:
            print(Fore.RED + f"   ❌ Failed to retrieve data (Status: {response.status_code})")
            return False
    except Exception as e:
        print(Fore.RED + f"   ❌ Error: {e}")
        return False

def check_web_server():
    """Check if web server is running"""
    print(Fore.YELLOW + "\n4. Checking Web Server..." + Style.RESET_ALL)
    try:
        response = requests.get("http://localhost:8080/index.html", timeout=3)
        if response.status_code == 200:
            print(Fore.GREEN + "   ✅ Web server is running")
            print("   🌐 Dashboard available at: http://localhost:8080/index.html")
            return True
        else:
            print(Fore.YELLOW + "   ⚠️  Web server returned unexpected status")
            return False
    except requests.exceptions.ConnectionError:
        print(Fore.YELLOW + "   ⚠️  Web server not running on port 8080")
        print("   Run: python -m http.server 8080")
        return False
    except Exception as e:
        print(Fore.RED + f"   ❌ Error: {e}")
        return False

def print_summary(results):
    """Print summary of all checks"""
    print("\n" + "="*60)
    print(Fore.CYAN + "SUMMARY" + Style.RESET_ALL)
    print("="*60)
    
    all_good = all(results.values())
    
    if all_good:
        print(Fore.GREEN + Style.BRIGHT + "\n🎉 Everything is working perfectly!")
        print(Fore.GREEN + "Your CQI Dashboard is ready to use.")
        print(f"\n📊 Open your dashboard at: {Fore.CYAN}http://localhost:8080/index.html")
    else:
        print(Fore.YELLOW + "\n⚠️  Some components need attention:")
        
        if not results['api']:
            print(Fore.RED + "\n   ❌ Flask API is not running")
            print("      Solution: Run 'python app.py' in terminal")
        
        if results['api'] and not results['snowflake']:
            print(Fore.RED + "\n   ❌ Snowflake connection failed")
            print("      Check:")
            print("      - private_key.txt exists in current directory")
            print("      - Snowflake credentials are correct")
            print("      - Network connection to Snowflake")
        
        if not results['web']:
            print(Fore.YELLOW + "\n   ⚠️  Web server not running")
            print("      Solution: Run 'python -m http.server 8080' in another terminal")
    
    print("\n" + "="*60 + "\n")

def main():
    """Main function to run all checks"""
    print_header()
    
    results = {
        'api': False,
        'snowflake': False,
        'data': False,
        'web': False
    }
    
    # Check API
    results['api'] = check_api_health()
    
    # Only check Snowflake and data if API is running
    if results['api']:
        results['snowflake'] = check_snowflake_connection()
        if results['snowflake']:
            results['data'] = check_data_retrieval()
    
    # Check web server
    results['web'] = check_web_server()
    
    # Print summary
    print_summary(results)

if __name__ == "__main__":
    # Check if required modules are installed
    try:
        import requests
        import colorama
    except ImportError:
        print("Installing required modules for status checker...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "colorama"])
        print("Modules installed. Please run this script again.")
        sys.exit(0)
    
    main()