#!/usr/bin/env python3
"""
test_snowflake.py - Test Snowflake connection and data retrieval
Run this to diagnose connection and data issues
"""

import snowflake.connector as sc
import pandas as pd
from datetime import datetime, timedelta
import sys
import traceback

# ANSI color codes for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'
BOLD = '\033[1m'


def print_section(title):
    """Print a section header"""
    print(f"\n{BLUE}{BOLD}{'='*60}{RESET}")
    print(f"{BLUE}{BOLD}{title}{RESET}")
    print(f"{BLUE}{BOLD}{'='*60}{RESET}")


def test_connection():
    """Test basic Snowflake connection"""
    print_section("1. TESTING SNOWFLAKE CONNECTION")

    try:
        print(f"{YELLOW}Attempting to connect to Snowflake...{RESET}")

        conn_params = {
            'account': 'nsasprd.east-us-2.privatelink',
            'user': 'm69382',
            'private_key_file': 'private_key.txt',
            'private_key_file_pwd': 'KsX.fVfg3_y0Ti5ewb0FNiPUc5kfDdJZws0tdgA.',
            'warehouse': 'USR_REPORTING_WH',
            'database': 'PRD_MOBILITY',
            'schema': 'PRD_MOBILITYSCORECARD_VIEWS'
        }

        conn = sc.connect(**conn_params)
        print(f"{GREEN}✅ Successfully connected to Snowflake!{RESET}")

        # Test current context
        cur = conn.cursor()
        cur.execute(
            "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER()")
        result = cur.fetchone()

        print(f"\n{BOLD}Connection Details:{RESET}")
        print(f"  Warehouse: {result[0]}")
        print(f"  Database:  {result[1]}")
        print(f"  Schema:    {result[2]}")
        print(f"  User:      {result[3]}")

        cur.close()
        return conn

    except FileNotFoundError:
        print(f"{RED}❌ Error: private_key.txt file not found!{RESET}")
        print(f"   Make sure private_key.txt is in the current directory")
        return None
    except Exception as e:
        print(f"{RED}❌ Connection failed!{RESET}")
        print(f"   Error: {str(e)}")
        traceback.print_exc()
        return None


def check_table_exists(conn):
    """Check if the CQI table/view exists"""
    print_section("2. CHECKING TABLE/VIEW EXISTS")

    try:
        cur = conn.cursor()

        # Check if table exists
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'PRD_MOBILITYSCORECARD_VIEWS' 
        AND TABLE_NAME = 'CQI2025_CQX_CONTRIBUTION'
        """

        cur.execute(query)
        count = cur.fetchone()[0]

        if count > 0:
            print(f"{GREEN}✅ Table CQI2025_CQX_CONTRIBUTION exists!{RESET}")

            # Get table type
            query2 = """
            SELECT TABLE_TYPE 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'PRD_MOBILITYSCORECARD_VIEWS' 
            AND TABLE_NAME = 'CQI2025_CQX_CONTRIBUTION'
            """
            cur.execute(query2)
            table_type = cur.fetchone()[0]
            print(f"   Table Type: {table_type}")

            cur.close()
            return True
        else:
            print(f"{RED}❌ Table CQI2025_CQX_CONTRIBUTION not found!{RESET}")

            # List available tables
            print(f"\n{YELLOW}Available tables in schema:{RESET}")
            query3 = """
            SELECT TABLE_NAME, TABLE_TYPE 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'PRD_MOBILITYSCORECARD_VIEWS'
            LIMIT 20
            """
            cur.execute(query3)
            tables = cur.fetchall()
            for table in tables:
                print(f"  - {table[0]} ({table[1]})")

            cur.close()
            return False

    except Exception as e:
        print(f"{RED}❌ Error checking table: {str(e)}{RESET}")
        traceback.print_exc()
        return False


def check_table_structure(conn):
    """Check the structure of the table"""
    print_section("3. CHECKING TABLE STRUCTURE")

    try:
        cur = conn.cursor()

        # Get column information
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'PRD_MOBILITYSCORECARD_VIEWS' 
        AND TABLE_NAME = 'CQI2025_CQX_CONTRIBUTION'
        ORDER BY ORDINAL_POSITION
        """

        cur.execute(query)
        columns = cur.fetchall()

        if columns:
            print(f"{GREEN}✅ Found {len(columns)} columns:{RESET}")
            print(f"\n{BOLD}Key Columns:{RESET}")

            # Show important columns
            important_cols = ['USID', 'METRICNAME', 'CQITARGET', 'FOCUSAREA_L1CQIACTUAL',
                              'SUBMKT', 'CQECLUSTER', 'PERIODSTART', 'PERIODEND', 'VENDOR']

            for col in columns:
                if col[0] in important_cols:
                    nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                    print(f"  ✓ {col[0]:<25} {col[1]:<15} {nullable}")

            # Show if any expected columns are missing
            found_cols = [col[0] for col in columns]
            missing = [c for c in important_cols if c not in found_cols]
            if missing:
                print(
                    f"\n{YELLOW}⚠️  Missing expected columns: {', '.join(missing)}{RESET}")
        else:
            print(f"{RED}❌ No column information found!{RESET}")

        cur.close()
        return len(columns) > 0

    except Exception as e:
        print(f"{RED}❌ Error checking structure: {str(e)}{RESET}")
        traceback.print_exc()
        return False


def check_data_count(conn):
    """Check if table has any data"""
    print_section("4. CHECKING DATA COUNT")

    try:
        cur = conn.cursor()

        # Get total row count
        print(f"{YELLOW}Counting total rows...{RESET}")
        cur.execute("SELECT COUNT(*) FROM CQI2025_CQX_CONTRIBUTION")
        total_count = cur.fetchone()[0]

        if total_count > 0:
            print(f"{GREEN}✅ Table contains {total_count:,} total rows{RESET}")

            # Get recent data count
            print(f"\n{YELLOW}Checking recent data...{RESET}")
            query = """
            SELECT 
                COUNT(*) as row_count,
                MIN(PERIODSTART) as earliest_date,
                MAX(PERIODSTART) as latest_date
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= DATEADD(day, -30, CURRENT_DATE())
            """

            cur.execute(query)
            result = cur.fetchone()

            if result[0] > 0:
                print(
                    f"{GREEN}✅ Found {result[0]:,} rows in last 30 days{RESET}")
                print(f"   Earliest date: {result[1]}")
                print(f"   Latest date:   {result[2]}")
            else:
                print(f"{YELLOW}⚠️  No data in last 30 days{RESET}")

                # Check what date range has data
                query2 = """
                SELECT 
                    MIN(PERIODSTART) as earliest,
                    MAX(PERIODSTART) as latest
                FROM CQI2025_CQX_CONTRIBUTION
                """
                cur.execute(query2)
                dates = cur.fetchone()
                print(f"   Data available from {dates[0]} to {dates[1]}")
        else:
            print(f"{RED}❌ Table is empty (0 rows)!{RESET}")

        cur.close()
        return total_count

    except Exception as e:
        print(f"{RED}❌ Error counting data: {str(e)}{RESET}")
        traceback.print_exc()
        return 0


def test_sample_query(conn):
    """Test the actual query used by the Flask app"""
    print_section("5. TESTING ACTUAL QUERY")

    try:
        cur = conn.cursor()

        # Test the same query structure as Flask app
        query = """
        SELECT 
            CQI_YEAR,
            CONTRTYPE,
            PERIODSTART,
            PERIODEND,
            FOCUSLEV,
            FOCUSAREA,
            DETAILLEV,
            DETAILAREA,
            METRICNAME,
            METTYPE,
            NUM,
            DEN,
            EXTRAFAILURES,
            FOCUSAREA_L1CQIACTUAL,
            RAWTARGET,
            CQITARGET,
            IDXCONTR,
            METRICWT,
            EXP_CONST,
            TGTNUM,
            TGTDEN,
            USID,
            CQECLUSTER,
            VENDOR,
            SUBMKT,
            N2E_DATE
        FROM CQI2025_CQX_CONTRIBUTION
        WHERE 1=1
        ORDER BY IDXCONTR DESC 
        LIMIT 10
        """

        print(f"{YELLOW}Executing sample query (top 10 rows)...{RESET}")
        cur.execute(query)

        # Fetch results
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        if rows:
            print(f"{GREEN}✅ Query successful! Retrieved {len(rows)} rows{RESET}")

            # Convert to DataFrame for better display
            df = pd.DataFrame(rows, columns=columns)

            print(f"\n{BOLD}Sample Data:{RESET}")
            print(f"First row details:")
            first_row = df.iloc[0]

            key_fields = ['USID', 'METRICNAME', 'FOCUSAREA_L1CQIACTUAL',
                          'CQITARGET', 'IDXCONTR', 'VENDOR', 'CQECLUSTER', 'SUBMKT']

            for field in key_fields:
                if field in columns:
                    value = first_row[field]
                    print(f"  {field:<25}: {value}")

            # Check for NULL values in critical fields
            print(f"\n{BOLD}Data Quality Check:{RESET}")
            null_counts = df[key_fields].isnull().sum()
            for field in key_fields:
                if field in columns:
                    null_pct = (null_counts[field] / len(df)) * 100
                    if null_pct > 0:
                        print(
                            f"  {YELLOW}⚠️  {field}: {null_pct:.1f}% NULL values{RESET}")
                    else:
                        print(f"  {GREEN}✓ {field}: No NULL values{RESET}")
        else:
            print(f"{RED}❌ Query returned no results!{RESET}")

        cur.close()
        return len(rows) > 0

    except Exception as e:
        print(f"{RED}❌ Query failed: {str(e)}{RESET}")
        traceback.print_exc()
        return False


def test_filter_queries(conn):
    """Test the filter queries used by the dashboard"""
    print_section("6. TESTING FILTER QUERIES")

    try:
        cur = conn.cursor()

        # Test SUBMKT filter
        print(f"{YELLOW}Testing SUBMKT values...{RESET}")
        cur.execute("""
            SELECT DISTINCT SUBMKT 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE SUBMKT IS NOT NULL 
            LIMIT 10
        """)
        submarkets = [row[0] for row in cur.fetchall()]

        if submarkets:
            print(f"{GREEN}✅ Found {len(submarkets)} unique submarkets{RESET}")
            print(f"   Examples: {', '.join(submarkets[:5])}")
        else:
            print(f"{YELLOW}⚠️  No SUBMKT values found{RESET}")

        # Test CQECLUSTER filter
        print(f"\n{YELLOW}Testing CQECLUSTER values...{RESET}")
        cur.execute("""
            SELECT DISTINCT CQECLUSTER 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE CQECLUSTER IS NOT NULL 
            LIMIT 10
        """)
        clusters = [row[0] for row in cur.fetchall()]

        if clusters:
            print(f"{GREEN}✅ Found {len(clusters)} unique CQE clusters{RESET}")
            print(f"   Examples: {', '.join(clusters[:5])}")
        else:
            print(f"{YELLOW}⚠️  No CQECLUSTER values found{RESET}")

        # Test METRICNAME filter
        print(f"\n{YELLOW}Testing METRICNAME values...{RESET}")
        cur.execute("""
            SELECT DISTINCT METRICNAME 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE METRICNAME IS NOT NULL 
            LIMIT 10
        """)
        metrics = [row[0] for row in cur.fetchall()]

        if metrics:
            print(f"{GREEN}✅ Found {len(metrics)} unique metrics{RESET}")
            print(f"   Examples: {', '.join(metrics[:5])}")
        else:
            print(f"{YELLOW}⚠️  No METRICNAME values found{RESET}")

        cur.close()
        return True

    except Exception as e:
        print(f"{RED}❌ Filter query failed: {str(e)}{RESET}")
        traceback.print_exc()
        return False


def print_summary(results):
    """Print summary and recommendations"""
    print_section("SUMMARY & RECOMMENDATIONS")

    if all(results.values()):
        print(f"{GREEN}{BOLD}✅ All tests passed successfully!{RESET}")
        print(f"\nYour Snowflake connection is working and data is accessible.")
        print(f"The issue might be in the Flask app's error handling.")
        print(f"\n{BOLD}Next steps:{RESET}")
        print(f"1. Check the Flask app console for any hidden errors")
        print(f"2. Verify the date range in your dashboard filters")
        print(f"3. Check if the Flask app is using the correct query")
    else:
        print(f"{YELLOW}{BOLD}⚠️  Some issues detected:{RESET}\n")

        if not results['connection']:
            print(f"{RED}❌ Connection failed{RESET}")
            print(f"   - Check private_key.txt exists")
            print(f"   - Verify credentials and network access")

        if results['connection'] and not results['table_exists']:
            print(f"{RED}❌ Table not found{RESET}")
            print(f"   - Verify table name: CQI2025_CQX_CONTRIBUTION")
            print(f"   - Check schema: PRD_MOBILITYSCORECARD_VIEWS")

        if results['table_exists'] and results['row_count'] == 0:
            print(f"{RED}❌ Table is empty{RESET}")
            print(f"   - No data to display")
            print(f"   - Check if data load process has run")

        if results['row_count'] > 0 and not results['query_works']:
            print(f"{RED}❌ Query issues{RESET}")
            print(f"   - Check column names in query")
            print(f"   - Verify permissions on all columns")


def main():
    """Run all tests"""
    print(f"{BOLD}{BLUE}CQI DASHBOARD - SNOWFLAKE DIAGNOSTIC TEST{RESET}")
    print(f"Testing connection and data retrieval...")

    results = {
        'connection': False,
        'table_exists': False,
        'structure_ok': False,
        'row_count': 0,
        'query_works': False,
        'filters_work': False
    }

    # Test connection
    conn = test_connection()
    results['connection'] = conn is not None

    if conn:
        # Run remaining tests
        results['table_exists'] = check_table_exists(conn)

        if results['table_exists']:
            results['structure_ok'] = check_table_structure(conn)
            results['row_count'] = check_data_count(conn)

            if results['row_count'] > 0:
                results['query_works'] = test_sample_query(conn)
                results['filters_work'] = test_filter_queries(conn)

        # Close connection
        conn.close()

    # Print summary
    print_summary(results)


if __name__ == "__main__":
    main()
