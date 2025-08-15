#!/usr/bin/env python3
"""
dashboard_debug.py - Diagnose why the dashboard is stuck on loading
"""

import requests
import json
import time
from datetime import datetime, timedelta

def test_api_endpoints():
    """Test all API endpoints to find the issue"""
    
    print("="*60)
    print("CQI DASHBOARD DEBUG - Finding the Loading Issue")
    print("="*60)
    
    API_URL = "http://localhost:5000/api"
    
    # Test 1: Health check
    print("\n1. Testing API Health...")
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ API health: OK")
        else:
            print(f"❌ API health failed: {response.status_code}")
            return
    except Exception as e:
        print(f"❌ API not reachable: {e}")
        print("   Solution: Make sure 'python app.py' is running")
        return
    
    # Test 2: Test data endpoint with exact same request as frontend
    print("\n2. Testing Data Endpoint (Same as Frontend)...")
    try:
        # Use same date range as the frontend would use
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        
        # Same parameters as frontend sends
        params = {
            'submarket': '',
            'cqeCluster': '',
            'periodStart': start_date,
            'periodEnd': end_date,
            'metricName': '',
            'usid': ''
        }
        
        print(f"   Request URL: {API_URL}/data")
        print(f"   Parameters: {params}")
        print("   Testing with 15 second timeout...")
        
        start_time = time.time()
        response = requests.get(f"{API_URL}/data", params=params, timeout=15)
        end_time = time.time()
        
        print(f"   ⏱️  Response time: {end_time - start_time:.2f} seconds")
        
        if response.status_code == 200:
            print("✅ Request successful!")
            
            # Try to parse JSON
            try:
                data = response.json()
                print(f"✅ JSON parsing successful")
                print(f"   Data type: {type(data)}")
                
                if isinstance(data, list):
                    print(f"   Records returned: {len(data)}")
                    
                    if len(data) > 0:
                        print("   ✅ Data is not empty")
                        
                        # Check first record structure
                        first_record = data[0]
                        print(f"   Sample record keys: {list(first_record.keys())[:10]}")
                        
                        # Check for required fields
                        required_fields = ['USID', 'EXTRAFAILURES', 'METRICNAME']
                        missing_fields = [f for f in required_fields if f not in first_record]
                        
                        if missing_fields:
                            print(f"   ⚠️  Missing fields: {missing_fields}")
                        else:
                            print("   ✅ All required fields present")
                            
                        # Check for invalid values
                        failures = first_record.get('EXTRAFAILURES', 0)
                        print(f"   Sample EXTRAFAILURES: {failures} (type: {type(failures)})")
                        
                        if isinstance(failures, (int, float)):
                            print("   ✅ Numeric values are valid")
                        else:
                            print(f"   ❌ Invalid numeric value: {failures}")
                            
                    else:
                        print("   ⚠️  Empty data array - no records found")
                        print("   This could be why the table shows 'loading'")
                        
                elif isinstance(data, dict) and 'error' in data:
                    print(f"   ❌ API returned error: {data['error']}")
                else:
                    print(f"   ⚠️  Unexpected data format: {type(data)}")
                    
            except json.JSONDecodeError as e:
                print(f"   ❌ JSON parsing failed: {e}")
                print(f"   Raw response (first 200 chars): {response.text[:200]}")
                print("   This is likely causing the frontend to hang!")
                
        else:
            print(f"   ❌ HTTP error: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
    except requests.exceptions.Timeout:
        print("   ❌ REQUEST TIMEOUT (15 seconds)")
        print("   This is likely why the frontend is stuck on loading!")
        print("   The Snowflake query is taking too long.")
    except Exception as e:
        print(f"   ❌ Request failed: {e}")
    
    # Test 3: Test filters endpoint
    print("\n3. Testing Filters Endpoint...")
    try:
        response = requests.get(f"{API_URL}/filters", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("✅ Filters endpoint working")
            print(f"   Submarkets: {len(data.get('submarkets', []))}")
            print(f"   Clusters: {len(data.get('cqeClusters', []))}")
            print(f"   Metrics: {len(data.get('metricNames', []))}")
        else:
            print(f"❌ Filters failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Filters error: {e}")
    
    # Test 4: Test with smaller date range
    print("\n4. Testing with Smaller Date Range...")
    try:
        params = {
            'periodStart': end_date,  # Just today
            'periodEnd': end_date
        }
        
        print(f"   Testing single day: {end_date}")
        response = requests.get(f"{API_URL}/data", params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Single day query worked: {len(data)} records")
        else:
            print(f"❌ Single day query failed: {response.status_code}")
    except requests.exceptions.Timeout:
        print("❌ Even single day query timed out!")
    except Exception as e:
        print(f"❌ Single day error: {e}")
    
    # Test 5: Browser console check
    print("\n5. Browser Console Check")
    print("=" * 60)
    print("BROWSER DEBUGGING STEPS:")
    print("1. Open http://localhost:8080/index.html")
    print("2. Press F12 to open Developer Tools")
    print("3. Go to 'Console' tab")
    print("4. Look for any RED error messages")
    print("5. Go to 'Network' tab")
    print("6. Refresh the page")
    print("7. Look for any failed requests (red status codes)")
    print("8. Click on the '/data' request and check:")
    print("   - Status code")
    print("   - Response time")
    print("   - Response content")

def suggest_solutions():
    """Suggest potential solutions"""
    print("\n" + "="*60)
    print("POTENTIAL SOLUTIONS:")
    print("="*60)
    
    print("\n🔧 If the API request is timing out:")
    print("   1. Check if Snowflake connection is slow")
    print("   2. Try smaller date ranges in the dashboard")
    print("   3. Add indexes to the Snowflake table")
    print("   4. Increase timeout in the frontend")
    
    print("\n🔧 If there's a JSON parsing error:")
    print("   1. The NaN handling in app.py might need adjustment")
    print("   2. Check for any infinity values in the data")
    print("   3. Restart the Flask API: Ctrl+C then 'python app.py'")
    
    print("\n🔧 If the data is empty:")
    print("   1. Check the date range - use dates with actual data")
    print("   2. Verify the Snowflake table has recent data")
    print("   3. Check if the metric filters are too restrictive")
    
    print("\n🔧 If there are JavaScript errors:")
    print("   1. Check browser console (F12)")
    print("   2. Try a different browser")
    print("   3. Clear browser cache")

if __name__ == "__main__":
    test_api_endpoints()
    suggest_solutions()
    
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("1. Run this diagnostic and note any errors")
    print("2. Check browser console (F12) for JavaScript errors")
    print("3. If API timeout, try smaller date ranges")
    print("4. If JSON error, restart Flask API")
    print("="*60)