"""
CQI Dashboard Flask API - Fixed version with proper NaN handling
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
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging - Reduce Snowflake connector verbosity
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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


def clean_numeric_value(value):
    """Clean numeric values, handling NaN, Infinity, None, and negative values
    
    Negative values are set to 0 to ensure failure counts are never negative.
    """
    if value is None:
        return 0
    if pd.isna(value):
        return 0
    if isinstance(value, (float, np.floating)):
        if np.isinf(value):
            return 0
        if np.isnan(value):
            return 0
        # Set negative values to 0
        if value < 0:
            return 0
        # Convert to int if it's a whole number, otherwise keep as float
        if value == int(value):
            return int(value)
        return float(value)
    if isinstance(value, (int, np.integer)):
        # Set negative values to 0
        return max(0, int(value))
    # Try to convert string to number
    try:
        num_val = float(value)
        if np.isnan(num_val) or np.isinf(num_val):
            return 0
        # Set negative values to 0
        if num_val < 0:
            return 0
        if num_val == int(num_val):
            return int(num_val)
        return num_val
    except (ValueError, TypeError):
        return 0


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
        date_range = None

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

            # Get sample data with specific metrics
            cur.execute("""
                SELECT USID, METRICNAME, EXTRAFAILURES, IDXCONTR, VENDOR, CQECLUSTER, SUBMKT, PERIODSTART
                FROM CQI2025_CQX_CONTRIBUTION
                WHERE PERIODSTART IS NOT NULL
                AND METRICNAME IN (
                    'VOICE_CDR_RET_25', 'LTE_IQI_NS_ESO_25', 'LTE_IQI_RSRP_25',
                    'LTE_IQI_QUALITY_25', 'VOLTE_RAN_ACBACC_25_ALL', 'VOLTE_CDR_MOMT_ACC_25',
                    'ALLRAT_DACC_25', 'ALLRAT_DL_TPUT_25', 'ALLRAT_UL_TPUT_25',
                    'ALLRAT_DDR_25', 'VOLTE_WIFI_CDR_25'
                )
                ORDER BY IDXCONTR DESC NULLS LAST
                LIMIT 5
            """)
            sample_rows = cur.fetchall()

            for row in sample_rows:
                # Clean EXTRAFAILURES value (sets negative to 0)
                extrafailures = clean_numeric_value(row[2])
                # Keep IDXCONTR as is (can be negative)
                idxcontr = float(row[3]) if row[3] is not None else 0

                sample_data.append({
                    'USID': row[0],
                    'METRICNAME': row[1],
                    'EXTRAFAILURES': extrafailures,
                    'IDXCONTR': idxcontr,
                    'VENDOR': row[4],
                    'CLUSTER': row[5],
                    'SUBMKT': row[6],
                    'PERIODSTART': row[7].strftime('%Y-%m-%d %H:%M:%S') if row[7] else None
                })

        cur.close()
        conn.close()

        response_data = {
            'connection': 'success',
            'user': context[0],
            'database': context[1],
            'schema': context[2],
            'table_exists': table_exists,
            'total_rows': row_count,
            'recent_rows_7days': recent_count,
            'sample_data': sample_data
        }

        if date_range:
            response_data['date_range'] = {
                'earliest': date_range[0].strftime('%Y-%m-%d %H:%M:%S') if date_range[0] else None,
                'latest': date_range[1].strftime('%Y-%m-%d %H:%M:%S') if date_range[1] else None
            }

        return jsonify(response_data)

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
    """Get CQI data - aggregated by USID only when no metric filter, or by USID+METRICNAME when filtered"""
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

        # Determine if we're aggregating by USID only or USID+METRICNAME
        # If no metric filter, aggregate all metrics per USID
        aggregate_all_metrics = not metric_name

        if aggregate_all_metrics:
            # Aggregate ALL metrics per USID when no specific metric is filtered
            query = """
                SELECT 
                    USID,
                    'ALL' as METRICNAME,
                    AVG(EXTRAFAILURES) as AVG_EXTRAFAILURES,
                    SUM(EXTRAFAILURES) as TOTAL_EXTRAFAILURES,
                    AVG(IDXCONTR) as AVG_IDXCONTR,
                    SUM(IDXCONTR) as TOTAL_IDXCONTR,
                    COUNT(*) as RECORD_COUNT,
                    MAX(VENDOR) as VENDOR,
                    MAX(CQECLUSTER) as CQECLUSTER,
                    MAX(SUBMKT) as SUBMKT,
                    AVG(FOCUSAREA_L1CQIACTUAL) as AVG_ACTUAL,
                    AVG(CQITARGET) as AVG_TARGET,
                    MIN(PERIODSTART) as EARLIEST_PERIOD,
                    MAX(PERIODEND) as LATEST_PERIOD
                FROM CQI2025_CQX_CONTRIBUTION
                WHERE 1=1
            """
        else:
            # Aggregate by USID + METRICNAME when a specific metric is filtered
            query = """
                SELECT 
                    USID,
                    METRICNAME,
                    AVG(EXTRAFAILURES) as AVG_EXTRAFAILURES,
                    SUM(EXTRAFAILURES) as TOTAL_EXTRAFAILURES,
                    AVG(IDXCONTR) as AVG_IDXCONTR,
                    SUM(IDXCONTR) as TOTAL_IDXCONTR,
                    COUNT(*) as RECORD_COUNT,
                    MAX(VENDOR) as VENDOR,
                    MAX(CQECLUSTER) as CQECLUSTER,
                    MAX(SUBMKT) as SUBMKT,
                    AVG(FOCUSAREA_L1CQIACTUAL) as AVG_ACTUAL,
                    AVG(CQITARGET) as AVG_TARGET,
                    MIN(PERIODSTART) as EARLIEST_PERIOD,
                    MAX(PERIODEND) as LATEST_PERIOD
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

        # Handle datetime comparisons
        if period_start:
            query += " AND PERIODSTART >= %s"
            params.append(f"{period_start} 00:00:00")

        if period_end:
            query += " AND PERIODEND <= %s"
            params.append(f"{period_end} 23:59:59")

        if metric_name:
            # When a specific metric is selected, filter by it
            reverse_mapping = {v: k for k, v in metric_mapping.items()}
            actual_metric = reverse_mapping.get(metric_name, metric_name)
            query += " AND METRICNAME = %s"
            params.append(actual_metric)

        if usid:
            query += " AND USID = %s"
            params.append(usid)

        # Group by clause depends on aggregation mode
        if aggregate_all_metrics:
            # Group by USID only when aggregating all metrics
            query += """ 
                GROUP BY USID
                ORDER BY AVG_IDXCONTR DESC NULLS LAST
                LIMIT 1000
            """
        else:
            # Group by USID and METRICNAME when filtering by specific metric
            query += """ 
                GROUP BY USID, METRICNAME
                ORDER BY AVG_IDXCONTR DESC NULLS LAST
                LIMIT 1000
            """

        # Execute query
        conn = get_snowflake_connection()
        cur = conn.cursor()

        logger.info(
            f"Executing {'USID-only' if aggregate_all_metrics else 'USID+Metric'} aggregated query with {len(params)} parameters")

        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        # Fetch results
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()

        logger.info(f"Query returned {len(data)} aggregated rows")

        cur.close()
        conn.close()

        # Process results
        result = []
        for row in data:
            record = {}
            for i, col in enumerate(columns):
                value = row[i]

                # Handle different data types
                if col in ['AVG_EXTRAFAILURES', 'TOTAL_EXTRAFAILURES']:
                    # Clean failure values - sets negative to 0
                    record[col] = clean_numeric_value(value)
                elif col in ['AVG_IDXCONTR', 'TOTAL_IDXCONTR']:
                    # Clean contribution values - can be negative (keep as is for contribution)
                    record[col] = float(value) if value is not None else 0
                elif col in ['AVG_ACTUAL', 'AVG_TARGET']:
                    # Clean numeric averages
                    record[col] = clean_numeric_value(value)
                elif col == 'RECORD_COUNT':
                    # Keep count as integer
                    record[col] = int(value) if value else 0
                elif col in ['EARLIEST_PERIOD', 'LATEST_PERIOD']:
                    # Handle datetime fields
                    if value is not None:
                        if isinstance(value, (datetime, pd.Timestamp)):
                            record[col] = value.isoformat()
                        else:
                            record[col] = None
                    else:
                        record[col] = None
                else:
                    # String fields
                    record[col] = value

            # Add metric display name
            if aggregate_all_metrics or record.get('METRICNAME') == 'ALL':
                # When aggregating all metrics, display "All"
                record['METRIC_DISPLAY'] = 'All'
            elif record.get('METRICNAME') in metric_mapping:
                record['METRIC_DISPLAY'] = metric_mapping[record['METRICNAME']]
            else:
                record['METRIC_DISPLAY'] = record.get('METRICNAME', '')

            # For compatibility, also include EXTRAFAILURES as the average
            record['EXTRAFAILURES'] = record.get('AVG_EXTRAFAILURES', 0)
            record['IDXCONTR'] = record.get('AVG_IDXCONTR', 0)

            result.append(record)

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error fetching CQI data: {str(e)}")
        import traceback
        traceback.print_exc()
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

        # Define allowed metrics
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

        allowed_metrics = list(metric_mapping.keys())

        # Query for summary statistics
        query = f"""
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
            AND METRICNAME IN ({','.join(['%s'] * len(allowed_metrics))})
        """

        params = [start_date, end_date] + allowed_metrics
        cur.execute(query, params)
        result = cur.fetchone()

        summary = {
            'totalUsids': result[0] or 0,
            'totalRecords': result[1] or 0,
            'totalFailures': clean_numeric_value(result[2]),
            'avgFailures': clean_numeric_value(result[3]),
            'maxFailures': clean_numeric_value(result[4]),
            'criticalOffenders': result[5] or 0,
            'highOffenders': result[6] or 0,
            'mediumOffenders': result[7] or 0,
            'lowOffenders': result[8] or 0,
            'lastUpdated': datetime.now().isoformat()
        }

        cur.close()
        conn.close()

        return jsonify(summary)

    except Exception as e:
        logger.error(f"Error fetching summary stats: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/usid-detail', methods=['GET'])
def get_usid_detail():
    """Get detailed metric data for a specific USID over time"""
    try:
        # Get parameters
        usid = request.args.get('usid', '')
        period_start = request.args.get('periodStart', '')
        period_end = request.args.get('periodEnd', '')
        metric_name = request.args.get('metricName', '')

        if not usid:
            return jsonify({'error': 'USID is required'}), 400

        logger.info(f"USID detail request for {usid}")

        # Define metric mapping
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

        # Query for time series data
        query = """
            SELECT 
                USID,
                METRICNAME,
                DATE(PERIODSTART) as DATE,
                AVG(EXTRAFAILURES) as EXTRAFAILURES,
                MAX(VENDOR) as VENDOR,
                MAX(CQECLUSTER) as CQECLUSTER,
                MAX(SUBMKT) as SUBMKT
            FROM CQI2025_CQX_CONTRIBUTION
            WHERE USID = %s
        """

        allowed_metrics = list(metric_mapping.keys())
        query += f" AND METRICNAME IN ({','.join(['%s'] * len(allowed_metrics))})"
        params = [usid] + allowed_metrics

        # Add date filters
        if period_start:
            query += " AND PERIODSTART >= %s"
            params.append(f"{period_start} 00:00:00")

        if period_end:
            query += " AND PERIODEND <= %s"
            params.append(f"{period_end} 23:59:59")

        # Filter by specific metric if provided
        if metric_name:
            reverse_mapping = {v: k for k, v in metric_mapping.items()}
            actual_metric = reverse_mapping.get(metric_name, metric_name)
            query += " AND METRICNAME = %s"
            params.append(actual_metric)

        # Group by date and metric, order by date
        query += """
            GROUP BY USID, METRICNAME, DATE(PERIODSTART)
            ORDER BY DATE(PERIODSTART), METRICNAME
        """

        # Execute query
        conn = get_snowflake_connection()
        cur = conn.cursor()

        logger.info(f"Executing USID detail query for {usid}")
        cur.execute(query, params)

        # Fetch results
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()

        logger.info(f"Retrieved {len(data)} data points for USID {usid}")

        cur.close()
        conn.close()

        # Process results
        result = []
        for row in data:
            record = {}
            for i, col in enumerate(columns):
                value = row[i]

                if col == 'EXTRAFAILURES':
                    # Set negative values to 0
                    record[col] = clean_numeric_value(value)
                elif col == 'DATE':
                    # Format date as ISO string
                    if value:
                        if hasattr(value, 'isoformat'):
                            record['PERIODSTART'] = value.isoformat()
                        else:
                            record['PERIODSTART'] = str(value)
                    else:
                        record['PERIODSTART'] = None
                else:
                    record[col] = value

            # Add metric display name
            if record.get('METRICNAME') in metric_mapping:
                record['METRIC_DISPLAY'] = metric_mapping[record['METRICNAME']]
            else:
                record['METRIC_DISPLAY'] = record.get('METRICNAME', '')

            result.append(record)

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error fetching USID detail data: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Run the Flask app
    print("🚀 Starting CQI Dashboard API Server...")
    print("📊 API will be available at: http://localhost:5000")
    print("✅ Snowflake connection configured")
    print("📇 Verbose logging suppressed - only warnings/errors will show")
    print("-" * 50)

    # Set debug=False for production to reduce logging
    app.run(debug=False, host='0.0.0.0', port=5000)
