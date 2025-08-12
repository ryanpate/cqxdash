#!/usr/bin/env python3
"""
debug_datetime.py - Debug datetime issues with PERIODSTART/PERIODEND
"""

import snowflake.connector as sc
import pandas as pd
from datetime import datetime, timedelta

def test_datetime_queries():
    """Test different datetime query formats"""
    
    print("="*60)
    print("DATETIME DEBUG FOR CQI DASHBOARD")
    print("="*60)
    
    try:
        # Connect to Snowflake
        print("\n1. Connecting to Snowflake...")
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
        print("✅ Connected successfully")
        
        # Check data types of date columns
        print("\n2. Checking column data types...")
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'PRD_MOBILITYSCORECARD_VIEWS' 
            AND TABLE_NAME = 'CQI2025_CQX_CONTRIBUTION'
            AND COLUMN_NAME IN ('PERIODSTART', 'PERIODEND')
        """)
        for row in cur.fetchall():
            print(f"   {row[0]}: {row[1]}")
        
        # Get sample of actual datetime values
        print("\n3. Sample datetime values from table:")
        cur.execute("""
            SELECT PERIODSTART, PERIODEND 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE PERIODSTART IS NOT NULL 
            LIMIT 5
        """)
        for i, row in enumerate(cur.fetchall(), 1):
            print(f"   Row {i}:")
            print(f"      PERIODSTART: {row[0]} (type: {type(row[0]).__name__})")
            print(f"      PERIODEND:   {row[1]} (type: {type(row[1]).__name__})")
        
        # Get date range of data
        print("\n4. Date range in table:")
        cur.execute("""
            SELECT 
                MIN(PERIODSTART) as earliest,
                MAX(PERIODSTART) as latest,
                COUNT(*) as total_rows
            FROM CQI2025_CQX_CONTRIBUTION
        """)
        result = cur.fetchone()
        print(f"   Earliest: {result[0]}")
        print(f"   Latest:   {result[1]}")
        print(f"   Total rows: {result[2]:,}")
        
        # Test different query formats
        print("\n5. Testing different date filter formats:")
        
        # Test 1: Date string comparison
        print("\n   Test 1: Simple date string ('2025-08-10')")
        cur.execute("""
            SELECT COUNT(*) 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE PERIODSTART >= '2025-08-10'
        """)
        count1 = cur.fetchone()[0]
        print(f"   Result: {count1:,} rows")
        
        # Test 2: DateTime string comparison
        print("\n   Test 2: DateTime string ('2025-08-10 00:00:00')")
        cur.execute("""
            SELECT COUNT(*) 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE PERIODSTART >= '2025-08-10 00:00:00'
        """)
        count2 = cur.fetchone()[0]
        print(f"   Result: {count2:,} rows")
        
        # Test 3: Date casting
        print("\n   Test 3: DATE cast (DATE(PERIODSTART) >= '2025-08-10')")
        cur.execute("""
            SELECT COUNT(*) 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE DATE(PERIODSTART) >= '2025-08-10'
        """)
        count3 = cur.fetchone()[0]
        print(f"   Result: {count3:,} rows")
        
        # Test 4: Parameterized query (like Flask uses)
        print("\n   Test 4: Parameterized query with datetime string")
        date_param = '2025-08-10 00:00:00'
        cur.execute(
            "SELECT COUNT(*) FROM CQI2025_CQX_CONTRIBUTION WHERE PERIODSTART >= %s",
            (date_param,)
        )
        count4 = cur.fetchone()[0]
        print(f"   Result: {count4:,} rows")
        
        # Test actual Flask query
        print("\n6. Testing actual Flask query with parameters:")
        query = """
            SELECT 
                USID, METRICNAME, FOCUSAREA_L1CQIACTUAL, CQITARGET, 
                PERIODSTART, PERIODEND, SUBMKT, CQECLUSTER
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE 1=1
        """
        
        # Add date filters
        params = []
        period_start = '2025-08-10'
        period_end = '2025-08-12'
        
        if period_start:
            query += " AND PERIODSTART >= %s"
            params.append(f"{period_start} 00:00:00")
        
        if period_end:
            query += " AND PERIODEND <= %s"
            params.append(f"{period_end} 23:59:59")
        
        query += " ORDER BY IDXCONTR DESC LIMIT 10"
        
        print(f"   Query: {query}")
        print(f"   Parameters: {params}")
        
        cur.execute(query, params)
        results = cur.fetchall()
        print(f"   Results: {len(results)} rows returned")
        
        if results:
            print("\n   Sample result:")
            row = results[0]
            print(f"      USID: {row[0]}")
            print(f"      METRIC: {row[1]}")
            print(f"      ACTUAL: {row[2]}")
            print(f"      TARGET: {row[3]}")
            print(f"      PERIODSTART: {row[4]}")
            print(f"      PERIODEND: {row[5]}")
        
        # Check for recent data
        print("\n7. Checking for recent data (last 30 days):")
        cur.execute("""
            SELECT 
                DATE(PERIODSTART) as date,
                COUNT(*) as row_count
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            GROUP BY DATE(PERIODSTART)
            ORDER BY date DESC
            LIMIT 10
        """)
        recent_data = cur.fetchall()
        if recent_data:
            print("   Recent dates with data:")
            for row in recent_data:
                print(f"      {row[0]}: {row[1]:,} rows")
        else:
            print("   No data in last 30 days")
            
            # Check what dates DO have data
            print("\n   Checking what dates have data:")
            cur.execute("""
                SELECT 
                    DATE(PERIODSTART) as date,
                    COUNT(*) as row_count
                FROM CQI2025_CQX_CONTRIBUTION
                WHERE PERIODSTART IS NOT NULL
                GROUP BY DATE(PERIODSTART)
                ORDER BY date DESC
                LIMIT 10
            """)
            any_data = cur.fetchall()
            if any_data:
                print("   Most recent dates with data:")
                for row in any_data:
                    print(f"      {row[0]}: {row[1]:,} rows")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*60)
        print("RECOMMENDATIONS:")
        print("="*60)
        print("\n1. The Flask API has been updated to handle datetime correctly")
        print("2. Restart your Flask app: python app.py")
        print("3. In the dashboard, try these date ranges:")
        if recent_data:
            print(f"   - Start: {recent_data[-1][0]}")
            print(f"   - End: {recent_data[0][0]}")
        elif any_data:
            print(f"   - Start: {any_data[-1][0]}")
            print(f"   - End: {any_data[0][0]}")
        print("\n4. If still no data, check the actual date range of your data above")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_datetime_queries()