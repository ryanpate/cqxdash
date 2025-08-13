"""
CQI Dashboard Flask API
Connects to Snowflake and serves data to the web dashboard
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector as sc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from functools import lru_cache
import logging

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging - Reduce Snowflake connector verbosity
logging.basicConfig(level=logging.WARNING)  # Only show warnings and errors
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Keep app logs at INFO level

# Suppress Snowflake connector INFO logs
snowflake_logger = logging.getLogger('snowflake.connector')
snowflake_logger.setLevel(logging.WARNING)

# Suppress werkzeug INFO logs (Flask HTTP requests)
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'account': 'nsasprd.east-us-2.privatelink',
    'user': 'm69382',
    'private_key_file': 'private_key.txt',
    'private_key_file_pwd': 'KsX.fVfg3_y0Ti5ewb0FNiPUc5kfDdJZws0tdgA.',
    'warehouse': 'USR_REPORTING_WH',
    'database': 'PRD_MOBILITY',
    'schema': 'PRD_MOBILITYSCORECARD_VIEWS'
}


def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    try:
        conn = sc.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


@app.route('/api/test', methods=['GET'])
def test_connection():
    """Test endpoint to verify Snowflake connection and data"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Test 1: Basic connection
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        context = cur.fetchone()

        # Test 2: Check table exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'PRD_MOBILITYSCORECARD_VIEWS' 
            AND TABLE_NAME = 'CQI2025_CQX_CONTRIBUTION'
        """)
        table_exists = cur.fetchone()[0] > 0

        # Test 3: Count rows
        row_count = 0
        recent_count = 0
        sample_data = []

        if table_exists:
            cur.execute("SELECT COUNT(*) FROM CQI2025_CQX_CONTRIBUTION")
            row_count = cur.fetchone()[0]

            # Get recent data count
            cur.execute("""
                SELECT COUNT(*) 
                FROM CQI2025_CQX_CONTRIBUTION
                WHERE PERIODSTART >= DATEADD(day, -7, CURRENT_TIMESTAMP())
            """)
            recent_count = cur.fetchone()[0]

            # Get date range of data
            cur.execute("""
                SELECT 
                    MIN(PERIODSTART) as earliest,
                    MAX(PERIODSTART) as latest
                FROM CQI2025_CQX_CONTRIBUTION
            """)
            date_range = cur.fetchone()

            # Get sample data
            cur.execute("""
                SELECT USID, METRICNAME, EXTRAFAILURES, VENDOR, CQECLUSTER, SUBMKT, PERIODSTART
                FROM CQI2025_CQX_CONTRIBUTION
                WHERE PERIODSTART IS NOT NULL
                ORDER BY EXTRAFAILURES DESC NULLS LAST
                LIMIT 5
            """)
            sample_rows = cur.fetchall()
            sample_data = []
            for row in sample_rows:
                # Handle potential None/NULL values
                extrafailures = row[2]
                if extrafailures is not None and pd.notna(extrafailures):
                    try:
                        extrafailures = int(float(extrafailures))
                    except (ValueError, TypeError):
                        extrafailures = 0
                else:
                    extrafailures = 0

                sample_data.append({
                    'USID': row[0],
                    'METRICNAME': row[1],
                    'EXTRAFAILURES': extrafailures,
                    'VENDOR': row[3],
                    'CLUSTER': row[4],
                    'SUBMKT': row[5],
                    'PERIODSTART': row[6].strftime('%Y-%m-%d %H:%M:%S') if row[6] else None
                })

        cur.close()
        conn.close()

        return jsonify({
            'connection': 'success',
            'user': context[0],
            'database': context[1],
            'schema': context[2],
            'table_exists': table_exists,
            'total_rows': row_count,
            'recent_rows_7days': recent_count,
            'date_range': {
                'earliest': date_range[0].strftime('%Y-%m-%d %H:%M:%S') if date_range[0] else None,
                'latest': date_range[1].strftime('%Y-%m-%d %H:%M:%S') if date_range[1] else None
            } if table_exists else None,
            'sample_data': sample_data
        })

    except Exception as e:
        logger.error(f"Test connection failed: {str(e)}")
        return jsonify({
            'connection': 'failed',
            'error': str(e),
            'error_type': type(e).__name__
        }), 500


@app.route('/api/filters', methods=['GET'])
@lru_cache(maxsize=1)
def get_filter_options():
    """Get available filter options from the database"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        filters = {}

        # Define allowed metrics with display names
        metric_mapping = {
            'VOICE_CDR_RET_25': 'V-CDR',
            'LTE_IQI_NS_ESO_25': 'NS/ESO',
            'LTE_IQI_RSRP_25': 'Quality RSRP',
            'LTE_IQI_QUALITY_25': 'Quality RSRQ',
            'VOLTE_RAN_ACBACC_25_ALL': 'V-ACC',
            'VOLTE_CDR_MOMT_ACC_25': 'V-ACC-E2E',
            'ALLRAT_DACC_25': 'D-ACC',
            'ALLRAT_DL_TPUT_25': 'DLTPUT',
            'ALLRAT_UL_TPUT_25': 'ULTPUT',
            'ALLRAT_DDR_25': 'D-RET',
            'VOLTE_WIFI_CDR_25': 'WIFI-RET'
        }

        # Get unique submarkets
        cur.execute("""
            SELECT DISTINCT SUBMKT 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE SUBMKT IS NOT NULL 
            ORDER BY SUBMKT
        """)
        filters['submarkets'] = [row[0] for row in cur.fetchall()]

        # Get unique CQE clusters
        cur.execute("""
            SELECT DISTINCT CQECLUSTER 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE CQECLUSTER IS NOT NULL 
            ORDER BY CQECLUSTER
        """)
        filters['cqeClusters'] = [row[0] for row in cur.fetchall()]

        # Return the display names for metrics
        filters['metricNames'] = list(metric_mapping.values())
        # Include mapping for reference
        filters['metricMapping'] = metric_mapping

        cur.close()
        conn.close()

        return jsonify(filters)

    except Exception as e:
        logger.error(f"Error fetching filter options: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/data', methods=['GET'])
def get_cqi_data():
    """Get CQI data based on filters"""
    try:
        # Log incoming request
        logger.info(
            f"Data request received with filters: {request.args.to_dict()}")

        # Get filter parameters from query string
        submarket = request.args.get('submarket', '')
        cqe_cluster = request.args.get('cqeCluster', '')
        period_start = request.args.get('periodStart', '')
        period_end = request.args.get('periodEnd', '')
        metric_name = request.args.get('metricName', '')
        usid = request.args.get('usid', '')

        # Define metric name mapping
        metric_mapping = {
            'VOICE_CDR_RET_25': 'V-CDR',
            'LTE_IQI_NS_ESO_25': 'NS/ESO',
            'LTE_IQI_RSRP_25': 'Quality RSRP',
            'LTE_IQI_QUALITY_25': 'Quality RSRQ',
            'VOLTE_RAN_ACBACC_25_ALL': 'V-ACC',
            'VOLTE_CDR_MOMT_ACC_25': 'V-ACC-E2E',
            'ALLRAT_DACC_25': 'D-ACC',
            'ALLRAT_DL_TPUT_25': 'DLTPUT',
            'ALLRAT_UL_TPUT_25': 'ULTPUT',
            'ALLRAT_DDR_25': 'D-RET',
            'VOLTE_WIFI_CDR_25': 'WIFI-RET'
        }

        # Build the query with filters
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
        """

        # Add filter for specific metrics only
        allowed_metrics = list(metric_mapping.keys())
        query += f" AND METRICNAME IN ({','.join(['%s'] * len(allowed_metrics))})"
        params = allowed_metrics.copy()

        # Add additional filters dynamically
        if submarket:
            query += " AND SUBMKT = %s"
            params.append(submarket)

        if cqe_cluster:
            query += " AND CQECLUSTER = %s"
            params.append(cqe_cluster)

        # Handle datetime comparisons for PERIODSTART and PERIODEND
        # Since the columns are datetime, we need to compare with datetime values
        if period_start:
            # For start date, use beginning of day (00:00:00)
            query += " AND PERIODSTART >= %s"
            params.append(f"{period_start} 00:00:00")

        if period_end:
            # For end date, use end of day (23:59:59)
            query += " AND PERIODEND <= %s"
            params.append(f"{period_end} 23:59:59")

        if metric_name:
            # Reverse lookup the actual metric name from display name
            reverse_mapping = {v: k for k, v in metric_mapping.items()}
            actual_metric = reverse_mapping.get(metric_name, metric_name)
            query += " AND METRICNAME = %s"
            params.append(actual_metric)

        if usid:
            query += " AND USID = %s"
            params.append(usid)

        # Order by EXTRAFAILURES (worst offenders first - highest failures)
        query += " ORDER BY EXTRAFAILURES DESC NULLS LAST LIMIT 1000"

        # Execute query
        conn = get_snowflake_connection()
        cur = conn.cursor()

        logger.info(f"Executing query with {len(params)} parameters")

        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        # Fetch results and convert to DataFrame
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()

        logger.info(f"Query returned {len(data)} rows")

        # Check if we got any data
        if len(data) == 0:
            logger.warning("No data returned from Snowflake query")
            # Try a simpler query to see if there's any data at all
            test_query = "SELECT COUNT(*) FROM CQI2025_CQX_CONTRIBUTION"
            cur.execute(test_query)
            total_count = cur.fetchone()[0]
            logger.info(f"Total rows in table: {total_count}")

        df = pd.DataFrame(data, columns=columns)

        cur.close()
        conn.close()

        # Convert DataFrame to JSON
        result = df.to_dict('records')

        # Apply metric name mapping and ensure EXTRAFAILURES is integer
        for record in result:
            # Map metric names to display names
            if 'METRICNAME' in record and record['METRICNAME'] in metric_mapping:
                record['METRIC_DISPLAY'] = metric_mapping[record['METRICNAME']]
            else:
                record['METRIC_DISPLAY'] = record.get('METRICNAME', '')

            # Convert EXTRAFAILURES to integer, handling NaN and None values
            if 'EXTRAFAILURES' in record:
                if record['EXTRAFAILURES'] is not None and pd.notna(record['EXTRAFAILURES']):
                    try:
                        record['EXTRAFAILURES'] = int(
                            float(record['EXTRAFAILURES']))
                    except (ValueError, TypeError):
                        record['EXTRAFAILURES'] = 0
                else:
                    record['EXTRAFAILURES'] = 0

            # Handle other numeric fields that might have NaN
            numeric_fields = ['NUM', 'DEN', 'FOCUSAREA_L1CQIACTUAL', 'RAWTARGET',
                              'CQITARGET', 'IDXCONTR', 'METRICWT', 'EXP_CONST',
                              'TGTNUM', 'TGTDEN', 'CQI_YEAR']

            for field in numeric_fields:
                if field in record:
                    if pd.isna(record[field]):
                        record[field] = None
                    elif isinstance(record[field], (float, np.float64, np.float32)):
                        # Convert numpy floats to Python floats to avoid JSON serialization issues
                        record[field] = float(record[field])
                    elif isinstance(record[field], (np.int64, np.int32)):
                        # Convert numpy ints to Python ints
                        record[field] = int(record[field])

            # Convert date columns to ISO format strings
            for key in ['PERIODSTART', 'PERIODEND', 'N2E_DATE']:
                if key in record and record[key] is not None:
                    if isinstance(record[key], (datetime, pd.Timestamp)):
                        record[key] = record[key].isoformat()
                    elif pd.isna(record[key]):
                        record[key] = None

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error fetching CQI data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/summary', methods=['GET'])
def get_summary_stats():
    """Get summary statistics for the dashboard"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Get date range for last 7 days
        end_date = datetime.now().strftime('%Y-%m-%d 23:59:59')
        start_date = (datetime.now() - timedelta(days=7)
                      ).strftime('%Y-%m-%d 00:00:00')

        # Query for summary statistics
        query = """
            SELECT 
                COUNT(DISTINCT USID) as total_usids,
                COUNT(*) as total_records,
                SUM(EXTRAFAILURES) as total_failures,
                AVG(EXTRAFAILURES) as avg_failures,
                MAX(EXTRAFAILURES) as max_failures,
                COUNT(CASE WHEN EXTRAFAILURES > 10000 THEN 1 END) as critical_offenders,
                COUNT(CASE WHEN EXTRAFAILURES BETWEEN 1001 AND 10000 THEN 1 END) as high_offenders,
                COUNT(CASE WHEN EXTRAFAILURES BETWEEN 101 AND 1000 THEN 1 END) as medium_offenders,
                COUNT(CASE WHEN EXTRAFAILURES <= 100 THEN 1 END) as low_offenders
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= %s AND PERIODSTART <= %s
        """

        cur.execute(query, (start_date, end_date))
        result = cur.fetchone()

        # Helper function to safely convert to int
        def safe_int(value):
            if value is None or pd.isna(value):
                return 0
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return 0

        summary = {
            'totalUsids': safe_int(result[0]),
            'totalRecords': safe_int(result[1]),
            'totalFailures': safe_int(result[2]),
            'avgFailures': safe_int(result[3]),
            'maxFailures': safe_int(result[4]),
            'criticalOffenders': safe_int(result[5]),
            'highOffenders': safe_int(result[6]),
            'mediumOffenders': safe_int(result[7]),
            'lowOffenders': safe_int(result[8]),
            'lastUpdated': datetime.now().isoformat()
        }

        cur.close()
        conn.close()

        return jsonify(summary)

    except Exception as e:
        logger.error(f"Error fetching summary stats: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/trends', methods=['GET'])
def get_trend_data():
    """Get trend data for visualization"""
    try:
        # Get parameters
        metric_name = request.args.get('metricName', '')
        days = int(request.args.get('days', 30))

        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Query for trend data
        query = """
            SELECT 
                DATE(PERIODSTART) as date,
                AVG(FOCUSAREA_L1CQIACTUAL) as avg_actual,
                AVG(CQITARGET) as avg_target,
                COUNT(DISTINCT USID) as usid_count
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE PERIODSTART >= DATEADD(day, -%s, CURRENT_DATE())
        """

        params = [days]

        if metric_name:
            query += " AND METRICNAME = %s"
            params.append(metric_name)

        query += " GROUP BY DATE(PERIODSTART) ORDER BY date"

        cur.execute(query, params)

        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=columns)

        cur.close()
        conn.close()

        # Convert to JSON
        result = df.to_dict('records')

        # Handle NaN values and date formatting
        for record in result:
            # Handle date
            if 'DATE' in record and record['DATE'] is not None:
                if hasattr(record['DATE'], 'isoformat'):
                    record['DATE'] = record['DATE'].isoformat()

            # Handle numeric fields
            for field in ['AVG_ACTUAL', 'AVG_TARGET', 'USID_COUNT']:
                if field in record:
                    if pd.isna(record[field]):
                        record[field] = None
                    elif isinstance(record[field], (float, np.float64)):
                        record[field] = float(record[field])
                    elif isinstance(record[field], (np.int64, np.int32)):
                        record[field] = int(record[field])

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error fetching trend data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/export', methods=['GET'])
def export_data():
    """Export data to CSV format"""
    try:
        # This would be similar to get_cqi_data but returns CSV
        # Get the data first
        data_response = get_cqi_data()
        data = data_response.get_json()

        if isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            csv_data = df.to_csv(index=False)

            response = app.response_class(
                response=csv_data,
                status=200,
                mimetype='text/csv',
                headers={
                    'Content-Disposition': f'attachment; filename=cqi_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                }
            )
            return response
        else:
            return jsonify({'error': 'No data to export'}), 404

    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Run the Flask app
    # In production, use a proper WSGI server like gunicorn
    print("🚀 Starting CQI Dashboard API Server...")
    print("📊 API will be available at: http://localhost:5000")
    print("✅ Snowflake connection configured")
    print("🔇 Verbose logging suppressed - only warnings/errors will show")
    print("-" * 50)

    # Set debug=False for production to reduce logging
    app.run(debug=False, host='0.0.0.0', port=5000)

"""
To run this API:

1. Install required packages:
   pip install flask flask-cors snowflake-connector-python pandas

2. Ensure your private_key.txt file is in the same directory

3. Run the Flask app:
   python app.py

4. The API will be available at http://localhost:5000

For production deployment:
- Use gunicorn or another WSGI server
- Add proper authentication/authorization
- Implement connection pooling for Snowflake
- Add caching for frequently accessed data
- Set up proper logging and monitoring
"""
