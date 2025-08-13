#!/usr/bin/env python3
"""
compare_aggregation.py - Shows the difference between raw and aggregated data
"""

import snowflake.connector as sc
import pandas as pd
from datetime import datetime, timedelta

def compare_data():
    """Compare raw vs aggregated data"""
    
    print("="*60)
    print("CQI DATA: RAW vs AGGREGATED COMPARISON")
    print("="*60)
    
    try:
        # Connect to Snowflake
        print("\nConnecting to Snowflake...")
        conn = sc.connect(
            account='nsasprd.east-us-2.privatelink',
            user='m69382',
            private_key_file='private_key.txt',
            private_key_file_pwd='KsX.fVfg3_y0Ti5ewb0FNiPUc5kfDdJZws0tdgA.',
            warehouse='USR_REPORTING_WH',
            database='PRD_MOBILITY',
            schema='PRD_MOBILITYSCORECARD_VIEWS'
        )
        cur = conn.cursor()
        print("✅ Connected")
        
        # Define the metrics we care about
        metrics = [
            'VOICE_CDR_RET_25', 'LTE_IQI_NS_ESO_25', 'LTE_IQI_RSRP_25',
            'LTE_IQI_QUALITY_25', 'VOLTE_RAN_ACBACC_25_ALL', 'VOLTE_CDR_MOMT_ACC_25',
            'ALLRAT_DACC_25', 'ALLRAT_DL_TPUT_25', 'ALLRAT_UL_TPUT_25',
            'ALLRAT_DDR_25', 'VOLTE_WIFI_CDR_25'
        ]
        
        # Set date range (last 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print(f"\nDate range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Query 1: Raw data (OLD approach - multiple rows per USID)
        print("\n" + "-"*60)
        print("RAW DATA (OLD APPROACH - Multiple rows per USID)")
        print("-"*60)
        
        raw_query = f"""
            SELECT 
                USID,
                METRICNAME,
                EXTRAFAILURES,
                PERIODSTART
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= %s
            AND PERIODSTART <= %s
            AND METRICNAME IN ({','.join(['%s'] * len(metrics))})
            AND USID = '184026'  -- Example USID
            AND METRICNAME = 'ALLRAT_UL_TPUT_25'  -- Example metric
            ORDER BY PERIODSTART
            LIMIT 20
        """
        
        params = [
            start_date.strftime('%Y-%m-%d 00:00:00'),
            end_date.strftime('%Y-%m-%d 23:59:59')
        ] + metrics
        
        cur.execute(raw_query, params)
        raw_data = cur.fetchall()
        
        if raw_data:
            print("\nExample: USID 184026 with metric ALLRAT_UL_TPUT_25")
            print("(Shows multiple entries for the same USID across different dates)")
            print("\n{:<10} {:<20} {:<15} {:<20}".format(
                "USID", "METRIC", "FAILURES", "DATE"
            ))
            print("-" * 65)
            
            total_failures = 0
            for row in raw_data:
                failures = float(row[2]) if row[2] else 0
                total_failures += failures
                print("{:<10} {:<20} {:<15,.0f} {:<20}".format(
                    row[0], 
                    row[1][:20], 
                    failures,
                    row[3].strftime('%Y-%m-%d %H:%M')
                ))
            
            print("-" * 65)
            print(f"Total rows: {len(raw_data)}")
            print(f"Total failures: {total_failures:,.0f}")
            print(f"Average failures: {total_failures/len(raw_data):,.0f}")
        else:
            print("No raw data found for example USID")
        
        # Query 2: Aggregated data (NEW approach - one row per USID+Metric)
        print("\n" + "-"*60)
        print("AGGREGATED DATA (NEW APPROACH - One row per USID+Metric)")
        print("-"*60)
        
        agg_query = f"""
            SELECT 
                USID,
                METRICNAME,
                AVG(EXTRAFAILURES) as AVG_FAILURES,
                SUM(EXTRAFAILURES) as TOTAL_FAILURES,
                COUNT(*) as RECORD_COUNT,
                MIN(PERIODSTART) as FIRST_DATE,
                MAX(PERIODSTART) as LAST_DATE
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= %s
            AND PERIODSTART <= %s
            AND METRICNAME IN ({','.join(['%s'] * len(metrics))})
            AND USID = '184026'  -- Same USID
            GROUP BY USID, METRICNAME
            ORDER BY AVG_FAILURES DESC
            LIMIT 20
        """
        
        cur.execute(agg_query, params)
        agg_data = cur.fetchall()
        
        if agg_data:
            print("\nSame USID (184026) - Now aggregated by metric:")
            print("(Each USID+Metric combination appears only once)")
            print("\n{:<10} {:<20} {:<12} {:<15} {:<8} {:<10}".format(
                "USID", "METRIC", "AVG_FAILURES", "TOTAL_FAILURES", "DAYS", "DATE_RANGE"
            ))
            print("-" * 90)
            
            for row in agg_data:
                avg_failures = float(row[2]) if row[2] else 0
                total_failures = float(row[3]) if row[3] else 0
                count = row[4]
                date_range = f"{row[5].strftime('%m/%d')}-{row[6].strftime('%m/%d')}"
                
                print("{:<10} {:<20} {:<12,.0f} {:<15,.0f} {:<8} {:<10}".format(
                    row[0],
                    row[1][:20],
                    avg_failures,
                    total_failures,
                    count,
                    date_range
                ))
            
            print("-" * 90)
            print(f"Total unique USID+Metric combinations: {len(agg_data)}")
        else:
            print("No aggregated data found")
        
        # Show the benefit at scale
        print("\n" + "="*60)
        print("BENEFITS OF AGGREGATION")
        print("="*60)
        
        # Count total raw rows
        count_query = f"""
            SELECT COUNT(*) as raw_count
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= %s
            AND PERIODSTART <= %s
            AND METRICNAME IN ({','.join(['%s'] * len(metrics))})
        """
        
        cur.execute(count_query, params[:2] + metrics)
        raw_count = cur.fetchone()[0]
        
        # Count aggregated rows
        agg_count_query = f"""
            SELECT COUNT(DISTINCT CONCAT(USID, '-', METRICNAME)) as agg_count
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= %s
            AND PERIODSTART <= %s
            AND METRICNAME IN ({','.join(['%s'] * len(metrics))})
        """
        
        cur.execute(agg_count_query, params[:2] + metrics)
        agg_count = cur.fetchone()[0]
        
        reduction = ((raw_count - agg_count) / raw_count * 100) if raw_count > 0 else 0
        
        print(f"\nRaw data rows (old approach): {raw_count:,}")
        print(f"Aggregated rows (new approach): {agg_count:,}")
        print(f"Data reduction: {reduction:.1f}%")
        print(f"\n✅ Result: {raw_count - agg_count:,} fewer rows to display!")
        print("✅ Cleaner ranking: Each USID appears once per metric")
        print("✅ Better performance: Less data to sort and render")
        print("✅ More meaningful: Shows average performance over time")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    compare_data()