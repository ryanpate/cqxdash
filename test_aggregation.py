#!/usr/bin/env python3
"""
test_aggregation.py - Test script to verify data aggregation is working
"""

import requests
import json
from datetime import datetime, timedelta

def test_aggregation():
    """Test the aggregated data endpoint"""
    
    print("="*60)
    print("TESTING CQI DASHBOARD AGGREGATION")
    print("="*60)
    
    API_URL = "http://localhost:5000/api"
    
    # Test 1: Health check
    print("\n1. Testing API health...")
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ API is running")
        else:
            print("❌ API health check failed")
            return
    except requests.exceptions.ConnectionError:
        print("❌ API is not running. Start it with: python app.py")
        return
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    # Test 2: Get aggregated data
    print("\n2. Testing aggregated data endpoint...")
    try:
        # Use a 7-day range
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        params = {
            'periodStart': start_date,
            'periodEnd': end_date
        }
        
        print(f"   Date range: {start_date} to {end_date}")
        
        response = requests.get(f"{API_URL}/data", params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Retrieved {len(data)} aggregated records")
            
            if len(data) > 0:
                # Check the structure of the first record
                first = data[0]
                print("\n3. Checking aggregation fields...")
                
                # Check for aggregation-specific fields
                required_fields = ['USID', 'METRICNAME', 'AVG_EXTRAFAILURES', 
                                 'TOTAL_EXTRAFAILURES', 'RECORD_COUNT']
                
                missing_fields = []
                for field in required_fields:
                    if field in first:
                        value = first[field]
                        if field == 'RECORD_COUNT':
                            print(f"   ✅ {field}: {value} records aggregated")
                        elif field == 'AVG_EXTRAFAILURES':
                            print(f"   ✅ {field}: {value:,.0f} average failures")
                        elif field == 'TOTAL_EXTRAFAILURES':
                            print(f"   ✅ {field}: {value:,.0f} total failures")
                        else:
                            print(f"   ✅ {field}: {value}")
                    else:
                        missing_fields.append(field)
                        print(f"   ❌ {field}: MISSING")
                
                if missing_fields:
                    print(f"\n❌ Missing aggregation fields: {', '.join(missing_fields)}")
                    print("   The data is not being aggregated properly.")
                else:
                    print("\n✅ All aggregation fields present!")
                
                # Check for duplicate USID+METRIC combinations
                print("\n4. Checking for proper aggregation...")
                combinations = set()
                duplicates = []
                
                for record in data:
                    combo = f"{record.get('USID')}-{record.get('METRICNAME')}"
                    if combo in combinations:
                        duplicates.append(combo)
                    combinations.add(combo)
                
                if duplicates:
                    print(f"   ❌ Found {len(duplicates)} duplicate USID+Metric combinations")
                    print(f"   Duplicates: {duplicates[:5]}")
                    print("   Data is NOT properly aggregated!")
                else:
                    print(f"   ✅ No duplicates found - each USID+Metric appears only once")
                    print("   Data is properly aggregated!")
                
                # Show sample aggregated data
                print("\n5. Sample aggregated records:")
                for i, record in enumerate(data[:3], 1):
                    usid = record.get('USID', 'N/A')
                    metric = record.get('METRIC_DISPLAY', record.get('METRICNAME', 'N/A'))
                    avg_failures = record.get('AVG_EXTRAFAILURES', 0)
                    total_failures = record.get('TOTAL_EXTRAFAILURES', 0)
                    count = record.get('RECORD_COUNT', 1)
                    
                    print(f"\n   Record {i}:")
                    print(f"   - USID: {usid}")
                    print(f"   - Metric: {metric}")
                    print(f"   - Average Failures: {avg_failures:,.0f}")
                    print(f"   - Total Failures: {total_failures:,.0f}")
                    print(f"   - Days/Records: {count}")
                    
                    # Verify the math
                    if count > 0:
                        calculated_avg = total_failures / count
                        if abs(calculated_avg - avg_failures) > 1:
                            print(f"   ⚠️ Math check: Calculated avg {calculated_avg:,.0f} != {avg_failures:,.0f}")
            
            else:
                print("   ⚠️ No data returned - try adjusting the date range")
                
        else:
            print(f"❌ Data endpoint failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    print("\n" + "="*60)
    print("AGGREGATION TEST COMPLETE")
    print("="*60)
    
    print("\nSummary:")
    print("- Each USID+Metric combination should appear only once")
    print("- RECORD_COUNT shows how many dates were aggregated")
    print("- AVG_EXTRAFAILURES is the average across all dates")
    print("- TOTAL_EXTRAFAILURES is the sum across all dates")
    print("\nYour dashboard now shows a cleaner ranking with one entry")
    print("per USID+Metric combination!")

if __name__ == "__main__":
    test_aggregation()