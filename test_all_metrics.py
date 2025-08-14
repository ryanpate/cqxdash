#!/usr/bin/env python3
"""
test_all_metrics.py - Test the "All" metrics aggregation behavior
"""

import requests
import json
from datetime import datetime, timedelta

def test_all_metrics_aggregation():
    """Test the All metrics aggregation vs specific metric filtering"""
    
    print("="*70)
    print("TESTING 'ALL' METRICS AGGREGATION")
    print("="*70)
    
    API_URL = "http://localhost:5000/api"
    
    # Set date range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    print(f"\nDate range: {start_date} to {end_date}")
    
    # Test 1: No metric filter (should show "All")
    print("\n" + "="*70)
    print("TEST 1: NO METRIC FILTER (Should aggregate all metrics per USID)")
    print("="*70)
    
    try:
        params = {
            'periodStart': start_date,
            'periodEnd': end_date
            # No metricName parameter - should aggregate all metrics
        }
        
        response = requests.get(f"{API_URL}/data", params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            if len(data) > 0:
                print(f"\n✅ Retrieved {len(data)} records")
                
                # Check first few records
                print("\nFirst 5 records:")
                print("-"*70)
                print(f"{'USID':<12} {'Metric':<15} {'Avg Failures':<15} {'Total Failures':<15} {'Records':<10}")
                print("-"*70)
                
                for i, record in enumerate(data[:5]):
                    usid = record.get('USID', 'N/A')
                    metric = record.get('METRIC_DISPLAY', record.get('METRICNAME', 'N/A'))
                    avg_failures = record.get('AVG_EXTRAFAILURES', 0)
                    total_failures = record.get('TOTAL_EXTRAFAILURES', 0)
                    record_count = record.get('RECORD_COUNT', 0)
                    
                    # Format numbers
                    if avg_failures >= 1000000:
                        avg_str = f"{avg_failures/1000000:.1f}M"
                    elif avg_failures >= 1000:
                        avg_str = f"{avg_failures/1000:.1f}K"
                    else:
                        avg_str = f"{avg_failures:.0f}"
                    
                    if total_failures >= 1000000:
                        total_str = f"{total_failures/1000000:.1f}M"
                    elif total_failures >= 1000:
                        total_str = f"{total_failures/1000:.1f}K"
                    else:
                        total_str = f"{total_failures:.0f}"
                    
                    print(f"{usid:<12} {metric:<15} {avg_str:<15} {total_str:<15} {record_count:<10}")
                
                # Check if all metrics show "All"
                all_metrics = [r.get('METRIC_DISPLAY', r.get('METRICNAME', '')) for r in data]
                unique_metrics = set(all_metrics)
                
                print(f"\n✅ Unique metric values in response: {unique_metrics}")
                
                if unique_metrics == {'All'} or unique_metrics == {'ALL'}:
                    print("✅ CORRECT: All records show 'All' for metric (aggregated across all metrics)")
                else:
                    print("❌ INCORRECT: Expected 'All' but found individual metrics")
                
                # Check for duplicate USIDs
                usids = [r.get('USID') for r in data]
                unique_usids = set(usids)
                
                if len(usids) == len(unique_usids):
                    print(f"✅ CORRECT: No duplicate USIDs found ({len(unique_usids)} unique USIDs)")
                else:
                    print(f"❌ INCORRECT: Found duplicate USIDs (expected each USID once)")
                
            else:
                print("⚠️ No data returned")
        else:
            print(f"❌ API error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 2: With specific metric filter (should show that metric)
    print("\n" + "="*70)
    print("TEST 2: WITH METRIC FILTER = 'V-CDR' (Should show only V-CDR)")
    print("="*70)
    
    try:
        params = {
            'periodStart': start_date,
            'periodEnd': end_date,
            'metricName': 'V-CDR'  # Specific metric filter
        }
        
        response = requests.get(f"{API_URL}/data", params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            if len(data) > 0:
                print(f"\n✅ Retrieved {len(data)} records")
                
                # Check first few records
                print("\nFirst 5 records:")
                print("-"*70)
                print(f"{'USID':<12} {'Metric':<15} {'Avg Failures':<15} {'Total Failures':<15} {'Records':<10}")
                print("-"*70)
                
                for i, record in enumerate(data[:5]):
                    usid = record.get('USID', 'N/A')
                    metric = record.get('METRIC_DISPLAY', record.get('METRICNAME', 'N/A'))
                    avg_failures = record.get('AVG_EXTRAFAILURES', 0)
                    total_failures = record.get('TOTAL_EXTRAFAILURES', 0)
                    record_count = record.get('RECORD_COUNT', 0)
                    
                    # Format numbers
                    if avg_failures >= 1000000:
                        avg_str = f"{avg_failures/1000000:.1f}M"
                    elif avg_failures >= 1000:
                        avg_str = f"{avg_failures/1000:.1f}K"
                    else:
                        avg_str = f"{avg_failures:.0f}"
                    
                    if total_failures >= 1000000:
                        total_str = f"{total_failures/1000000:.1f}M"
                    elif total_failures >= 1000:
                        total_str = f"{total_failures/1000:.1f}K"
                    else:
                        total_str = f"{total_failures:.0f}"
                    
                    print(f"{usid:<12} {metric:<15} {avg_str:<15} {total_str:<15} {record_count:<10}")
                
                # Check if all metrics show the filtered metric
                all_metrics = [r.get('METRIC_DISPLAY', r.get('METRICNAME', '')) for r in data]
                unique_metrics = set(all_metrics)
                
                print(f"\n✅ Unique metric values in response: {unique_metrics}")
                
                if 'V-CDR' in unique_metrics or 'VOICE_CDR_RET_25' in unique_metrics:
                    print("✅ CORRECT: Records show the filtered metric (V-CDR)")
                else:
                    print(f"❌ INCORRECT: Expected 'V-CDR' but found: {unique_metrics}")
                
            else:
                print("⚠️ No data returned (might not have V-CDR data)")
        else:
            print(f"❌ API error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print("\nExpected Behavior:")
    print("1. NO METRIC FILTER → Each USID appears once with 'All' in metric column")
    print("   - Aggregates all metrics for that USID")
    print("   - Shows total failures across all metrics")
    print("\n2. WITH METRIC FILTER → Each USID appears once for that specific metric")
    print("   - Shows only data for the selected metric")
    print("   - Aggregates across dates but not metrics")
    print("\nThis provides a cleaner ranking where:")
    print("- Default view shows worst USIDs overall (across all metrics)")
    print("- Filtered view shows worst USIDs for specific metric")

if __name__ == "__main__":
    test_all_metrics_aggregation()