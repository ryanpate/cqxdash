#!/usr/bin/env python3
"""
quick_test_nan_fix.py - Quick test to verify NaN handling is fixed
"""

import requests
import json

def test_api():
    """Quick test of the API endpoints"""
    
    print("Testing CQI Dashboard API for NaN issues...")
    print("=" * 50)
    
    API_URL = "http://localhost:5000/api"
    
    # Test 1: Health check
    print("\n1. Testing health endpoint...")
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Health check passed")
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return
    except requests.exceptions.ConnectionError:
        print("❌ API is not running. Start it with: python app.py")
        return
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    # Test 2: Test endpoint (most likely to show NaN issues)
    print("\n2. Testing /test endpoint for NaN values...")
    try:
        response = requests.get(f"{API_URL}/test", timeout=10)
        if response.status_code == 200:
            try:
                data = response.json()
                print("✅ JSON parsing successful")
                
                # Check sample data for issues
                if 'sample_data' in data and data['sample_data']:
                    print(f"   Found {len(data['sample_data'])} sample records")
                    for i, record in enumerate(data['sample_data']):
                        failures = record.get('EXTRAFAILURES', 0)
                        print(f"   Record {i+1}: USID={record.get('USID')}, Failures={failures}")
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing failed: {e}")
                print(f"   Response text: {response.text[:200]}...")
                return
        else:
            print(f"❌ Test endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 3: Data endpoint
    print("\n3. Testing /data endpoint...")
    try:
        params = {
            'periodStart': '2025-08-01',
            'periodEnd': '2025-08-12'
        }
        response = requests.get(f"{API_URL}/data", params=params, timeout=15)
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ Data endpoint successful - {len(data)} records")
                
                # Check for NaN in the response
                if data:
                    # Check first record
                    first = data[0]
                    failures = first.get('EXTRAFAILURES', 0)
                    print(f"   First record: USID={first.get('USID')}, Failures={failures}")
                    
                    # Verify it's a valid number
                    if isinstance(failures, (int, float)) and failures == failures:  # NaN != NaN
                        print("   ✅ No NaN values detected")
                    else:
                        print("   ⚠️  Potential numeric issue detected")
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing failed: {e}")
                print(f"   This is the NaN error - restart Flask with updated code")
        else:
            print(f"❌ Data endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 4: Filters endpoint
    print("\n4. Testing /filters endpoint...")
    try:
        response = requests.get(f"{API_URL}/filters", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Filters loaded successfully")
            if 'metricNames' in data:
                print(f"   Found {len(data['metricNames'])} metrics")
        else:
            print(f"❌ Filters endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("TEST COMPLETE")
    print("=" * 50)
    print("\nIf you see JSON parsing errors above:")
    print("1. Stop your Flask API (Ctrl+C)")
    print("2. Make sure you have the updated app.py")
    print("3. Restart Flask: python app.py")
    print("4. Run this test again")
    
    print("\nIf all tests pass:")
    print("✅ Your dashboard should work without NaN errors!")

if __name__ == "__main__":
    test_api()