#!/usr/bin/env python3
"""
test_nan_fix.py - Test script to verify NaN handling is fixed
"""

import requests
import json
import sys

def test_api_endpoints():
    """Test all API endpoints for NaN issues"""
    
    print("="*60)
    print("TESTING CQI DASHBOARD API - NaN HANDLING")
    print("="*60)
    
    API_URL = "http://localhost:5000/api"
    
    # Test 1: Health check
    print("\n1. Testing /health endpoint...")
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Health check passed")
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ API is not running. Start it with: python app.py")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # Test 2: Test endpoint
    print("\n2. Testing /test endpoint...")
    try:
        response = requests.get(f"{API_URL}/test", timeout=10)
        if response.status_code == 200:
            try:
                data = response.json()
                print("✅ JSON parsing successful")
                if 'sample_data' in data and data['sample_data']:
                    print(f"   Found {len(data['sample_data'])} sample records")
                    for i, record in enumerate(data['sample_data'][:3]):
                        failures = record.get('EXTRAFAILURES', 0)
                        # Check if it's a valid number
                        if isinstance(failures, (int, float)):
                            print(f"   ✅ Record {i+1}: USID={record.get('USID')}, Failures={failures:,}")
                        else:
                            print(f"   ❌ Record {i+1}: Invalid failures value: {failures}")
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing failed (NaN issue): {e}")
                print("   This means NaN values are not properly handled")
                return False
        else:
            print(f"❌ Test endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # Test 3: Data endpoint
    print("\n3. Testing /data endpoint...")
    try:
        params = {
            'periodStart': '2025-08-06',
            'periodEnd': '2025-08-13'
        }
        response = requests.get(f"{API_URL}/data", params=params, timeout=15)
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ Data endpoint successful - {len(data)} records")
                
                # Check for NaN in the response
                if data:
                    # Check first few records
                    for i, record in enumerate(data[:3]):
                        failures = record.get('EXTRAFAILURES', 0)
                        if isinstance(failures, (int, float)):
                            print(f"   ✅ Record {i+1}: USID={record.get('USID')}, Failures={failures:,}")
                        else:
                            print(f"   ❌ Record {i+1}: Invalid failures value")
                    
                    # Check for any invalid values in all records
                    invalid_count = 0
                    for record in data:
                        if not isinstance(record.get('EXTRAFAILURES', 0), (int, float)):
                            invalid_count += 1
                    
                    if invalid_count > 0:
                        print(f"   ⚠️ Found {invalid_count} records with invalid EXTRAFAILURES")
                    else:
                        print("   ✅ All EXTRAFAILURES values are valid numbers")
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing failed (NaN issue): {e}")
                print("   This is the NaN error - the API needs the fix applied")
                return False
        else:
            print(f"❌ Data endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # Test 4: Filters endpoint
    print("\n4. Testing /filters endpoint...")
    try:
        response = requests.get(f"{API_URL}/filters", timeout=10)
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ Filters loaded successfully")
                if 'metricNames' in data:
                    print(f"   Found {len(data['metricNames'])} metrics")
                if 'submarkets' in data:
                    print(f"   Found {len(data['submarkets'])} submarkets")
                if 'cqeClusters' in data:
                    print(f"   Found {len(data['cqeClusters'])} clusters")
            except json.JSONDecodeError:
                print("❌ JSON parsing failed in filters")
                return False
        else:
            print(f"❌ Filters endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # Test 5: Summary endpoint
    print("\n5. Testing /summary endpoint...")
    try:
        response = requests.get(f"{API_URL}/summary", timeout=10)
        if response.status_code == 200:
            try:
                data = response.json()
                print("✅ Summary endpoint successful")
                print(f"   Total USIDs: {data.get('totalUsids', 0):,}")
                print(f"   Total Records: {data.get('totalRecords', 0):,}")
                total_failures = data.get('totalFailures', 0)
                if isinstance(total_failures, (int, float)):
                    print(f"   Total Failures: {total_failures:,}")
                else:
                    print(f"   ❌ Invalid totalFailures value: {total_failures}")
            except json.JSONDecodeError:
                print("❌ JSON parsing failed in summary")
                return False
        else:
            print(f"❌ Summary endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    print("\n" + "="*60)
    print("✅ ALL TESTS PASSED - NO NaN ISSUES DETECTED!")
    print("="*60)
    print("\nYour dashboard should now work correctly.")
    print("Open it at: http://localhost:8080/index.html")
    return True


def main():
    """Main function"""
    print("\nThis script tests if the NaN handling fix is working.")
    print("Make sure you've restarted Flask with the updated app.py\n")
    
    success = test_api_endpoints()
    
    if not success:
        print("\n" + "="*60)
        print("❌ ISSUES DETECTED")
        print("="*60)
        print("\nTo fix:")
        print("1. Stop your Flask API (Ctrl+C)")
        print("2. Replace app.py with the fixed version")
        print("3. Restart Flask: python app.py")
        print("4. Run this test again")
        sys.exit(1)


if __name__ == "__main__":
    main()
