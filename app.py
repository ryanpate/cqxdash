"""
CQI Dashboard Flask API - Using CSV for Submarket-Cluster Mapping
Reads submarket-cluster relationships from submkt_cqecluster_mapping.csv
"""

from dotenv import load_dotenv  # For loading .env file
import json
import logging
from functools import lru_cache
import os
from datetime import datetime, timedelta
import csv
import numpy as np
import pandas as pd
import snowflake.connector as sc
from flask import Flask, jsonify, request
from flask_cors import CORS
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Load environment variables from .env file if it exists
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Suppress Snowflake connector INFO logs
snowflake_logger = logging.getLogger('snowflake.connector')
snowflake_logger.setLevel(logging.WARNING)

# Suppress werkzeug INFO logs
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)

# Snowflake connection parameters from environment variables
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT', 'nsasprd.east-us-2.privatelink'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'USR_REPORTING_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'PRD_MOBILITY'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PRD_MOBILITYSCORECARD_VIEWS')
}

# Path to the mapping CSV file
MAPPING_CSV_PATH = 'submkt_cqecluster_mapping.csv'

# Handle authentication method
if os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'):
    # Use private key authentication
    SNOWFLAKE_CONFIG['private_key_file'] = os.getenv(
        'SNOWFLAKE_PRIVATE_KEY_PATH')
    if os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'):
        SNOWFLAKE_CONFIG['private_key_file_pwd'] = os.getenv(
            'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
elif os.getenv('SNOWFLAKE_PASSWORD'):
    # Use password authentication
    SNOWFLAKE_CONFIG['password'] = os.getenv('SNOWFLAKE_PASSWORD')
else:
    logger.warning(
        "No authentication method configured. Set either SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")


def validate_config():
    """Validate that required configuration is present"""
    required = ['account', 'user', 'warehouse', 'database', 'schema']
    missing = [key for key in required if not SNOWFLAKE_CONFIG.get(key)]

    if missing:
        logger.error(
            f"Missing required Snowflake configuration: {', '.join(missing)}")
        logger.error("Please set the following environment variables:")
        for key in missing:
            logger.error(f"  SNOWFLAKE_{key.upper()}")
        return False

    # Check for authentication
    has_auth = (
        'private_key_file' in SNOWFLAKE_CONFIG or 'password' in SNOWFLAKE_CONFIG)
    if not has_auth:
        logger.error("No authentication method configured.")
        logger.error(
            "Set either SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
        return False

    return True


def load_submarket_cluster_mapping():
    """Load the submarket-cluster mapping from CSV file"""
    mapping = {}

    # Check if CSV file exists
    if not os.path.exists(MAPPING_CSV_PATH):
        logger.warning(f"Mapping CSV file not found: {MAPPING_CSV_PATH}")
        logger.warning("Creating sample mapping file...")

        # Create a sample CSV file if it doesn't exist
        sample_data = [
            ['SUBMKT', 'CQECLUSTER'],
            ['NYC', 'CQE_NYC_MANHATTAN'],
            ['NYC', 'CQE_NYC_BROOKLYN'],
            ['LA', 'CQE_LA_DOWNTOWN'],
            ['LA', 'CQE_LA_HOLLYWOOD'],
            ['Chicago', 'CQE_CHI_NORTH'],
            ['Chicago', 'CQE_CHI_SOUTH'],
        ]

        with open(MAPPING_CSV_PATH, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(sample_data)

        logger.info(f"Sample mapping file created: {MAPPING_CSV_PATH}")

    # Read the CSV file
    try:
        with open(MAPPING_CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                submarket = row.get('SUBMKT', '').strip()
                cluster = row.get('CQECLUSTER', '').strip()

                if submarket and cluster:
                    if submarket not in mapping:
                        mapping[submarket] = []
                    mapping[submarket].append(cluster)

        logger.info(
            f"Loaded mapping for {len(mapping)} submarkets from {MAPPING_CSV_PATH}")

    except Exception as e:
        logger.error(f"Error reading mapping CSV: {str(e)}")
        return {}

    return mapping


def clean_numeric_value(value):
    """Clean numeric values, handling NaN, Infinity, None, and negative values"""
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
        # Convert to int if it's a whole number
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


def clean_contribution_value(value):
    """Clean contribution values (can be negative)"""
    if value is None or pd.isna(value):
        return 0

    if isinstance(value, (float, np.floating)):
        if np.isinf(value) or np.isnan(value):
            return 0
        return float(value)

    if isinstance(value, (int, np.integer)):
        return float(value)

    try:
        num_val = float(value)
        if np.isnan(num_val) or np.isinf(num_val):
            return 0
        return num_val
    except (ValueError, TypeError):
        return 0


def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    if not validate_config():
        raise ValueError(
            "Invalid Snowflake configuration. Check environment variables.")

    try:
        conn = sc.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    config_valid = validate_config()
    csv_exists = os.path.exists(MAPPING_CSV_PATH)

    return jsonify({
        'status': 'healthy' if config_valid else 'unhealthy',
        'config_valid': config_valid,
        'mapping_csv_exists': csv_exists,
        'mapping_csv_path': MAPPING_CSV_PATH,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/test', methods=['GET'])
def test_connection():
    """Test endpoint to verify Snowflake connection and data"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Test basic connection
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        context = cur.fetchone()

        # Check table exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'PRD_MOBILITYSCORECARD_VIEWS' 
            AND TABLE_NAME = 'CQI2025_CQX_CONTRIBUTION'
        """)
        table_exists = cur.fetchone()[0] > 0

        # Count rows
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
                WHERE PERIODSTART >= DATEADD(day, -2, CURRENT_TIMESTAMP())
            """)
            recent_count = cur.fetchone()[0]

            # Get date range
            cur.execute("""
                SELECT 
                    MIN(PERIODSTART) as earliest,
                    MAX(PERIODSTART) as latest
                FROM CQI2025_CQX_CONTRIBUTION
            """)
            date_range = cur.fetchone()

            # Get sample data
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
                extrafailures = clean_numeric_value(row[2])
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

        # Load and test the CSV mapping
        mapping = load_submarket_cluster_mapping()
        mapping_info = {
            'total_submarkets': len(mapping),
            'total_mappings': sum(len(clusters) for clusters in mapping.values()),
            'sample_mappings': dict(list(mapping.items())[:3]) if mapping else {}
        }

        response_data = {
            'connection': 'success',
            'user': context[0],
            'database': context[1],
            'schema': context[2],
            'table_exists': table_exists,
            'total_rows': row_count,
            'recent_rows_2days': recent_count,
            'sample_data': sample_data,
            'csv_mapping': mapping_info
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
def get_filter_options():
    """Get available filter options from the database with CSV-based submarket-cluster mapping"""
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

        # Get unique submarkets from database
        cur.execute("""
            SELECT DISTINCT SUBMKT 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE SUBMKT IS NOT NULL 
            ORDER BY SUBMKT
        """)
        db_submarkets = [row[0] for row in cur.fetchall()]

        # Get all unique CQE clusters from database
        cur.execute("""
            SELECT DISTINCT CQECLUSTER 
            FROM CQI2025_CQX_CONTRIBUTION 
            WHERE CQECLUSTER IS NOT NULL 
            ORDER BY CQECLUSTER
        """)
        db_clusters = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()

        # Load the CSV mapping
        csv_mapping = load_submarket_cluster_mapping()

        # If CSV mapping exists, use it; otherwise fall back to database relationships
        if csv_mapping:
            # Use submarkets from CSV that also exist in the database
            csv_submarkets = set(csv_mapping.keys())
            db_submarkets_set = set(db_submarkets)

            # Intersection of CSV and database submarkets
            filters['submarkets'] = sorted(
                list(csv_submarkets & db_submarkets_set))

            # Log any mismatches for debugging
            csv_only = csv_submarkets - db_submarkets_set
            db_only = db_submarkets_set - csv_submarkets

            if csv_only:
                logger.warning(
                    f"Submarkets in CSV but not in database: {csv_only}")
            if db_only:
                logger.warning(
                    f"Submarkets in database but not in CSV: {db_only}")

            # Get all unique clusters from the CSV mapping
            all_csv_clusters = set()
            for clusters in csv_mapping.values():
                all_csv_clusters.update(clusters)

            # Use clusters that exist in both CSV and database
            db_clusters_set = set(db_clusters)
            filters['cqeClusters'] = sorted(
                list(all_csv_clusters & db_clusters_set))

            # Store the mapping for the frontend
            filters['submarketClusters'] = csv_mapping

            logger.info(
                f"Using CSV mapping: {len(filters['submarkets'])} submarkets, {len(filters['cqeClusters'])} clusters")
        else:
            # Fall back to database values if no CSV mapping
            filters['submarkets'] = db_submarkets
            filters['cqeClusters'] = db_clusters
            filters['submarketClusters'] = {}

            logger.warning(
                "No CSV mapping available, using all database values")

        # Return the display names for metrics
        filters['metricNames'] = list(metric_mapping.values())
        filters['metricMapping'] = metric_mapping

        return jsonify(filters)

    except Exception as e:
        logger.error(f"Error fetching filter options: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/data', methods=['GET'])
def get_cqi_data():
    """Get CQI data - aggregated by USID+METRICNAME with multi-select CQE Clusters support"""
    try:
        # Get filter parameters
        submarket = request.args.get('submarket', '')
        # Now accepts comma-separated values
        cqe_clusters_str = request.args.get('cqeClusters', '')
        period_start = request.args.get('periodStart', '')
        period_end = request.args.get('periodEnd', '')
        metric_name = request.args.get('metricName', '')
        usid = request.args.get('usid', '')
        sorting_criteria = request.args.get('sortingCriteria', 'contribution')

        # Parse multiple CQE clusters
        cqe_clusters = []
        if cqe_clusters_str:
            cqe_clusters = [c.strip()
                            for c in cqe_clusters_str.split(',') if c.strip()]

        logger.info(f"Data request with sorting: {sorting_criteria}")
        logger.info(f"Selected Submarket: {submarket}")
        logger.info(f"Selected CQE Clusters: {cqe_clusters}")

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

        # Determine aggregation mode
        aggregate_all_metrics = not metric_name

        if aggregate_all_metrics:
            # Aggregate ALL metrics per USID
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
            # Aggregate by USID + METRICNAME
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

        # Add filters
        allowed_metrics = list(metric_mapping.keys())
        query += f" AND METRICNAME IN ({','.join(['%s'] * len(allowed_metrics))})"
        params = allowed_metrics.copy()

        if submarket:
            query += " AND SUBMKT = %s"
            params.append(submarket)

        # Handle multiple CQE clusters
        if cqe_clusters:
            query += f" AND CQECLUSTER IN ({','.join(['%s'] * len(cqe_clusters))})"
            params.extend(cqe_clusters)

        if period_start:
            query += " AND PERIODSTART >= %s"
            params.append(f"{period_start} 00:00:00")

        if period_end:
            query += " AND PERIODEND <= %s"
            params.append(f"{period_end} 23:59:59")

        if metric_name:
            reverse_mapping = {v: k for k, v in metric_mapping.items()}
            actual_metric = reverse_mapping.get(metric_name, metric_name)
            query += " AND METRICNAME = %s"
            params.append(actual_metric)

        if usid:
            query += " AND USID = %s"
            params.append(usid)

        # Group by clause
        if aggregate_all_metrics:
            query += " GROUP BY USID"
        else:
            query += " GROUP BY USID, METRICNAME"

        # Order by based on sorting criteria
        if sorting_criteria == 'contribution':
            query += " ORDER BY AVG_IDXCONTR ASC NULLS LAST"
        else:
            query += " ORDER BY TOTAL_EXTRAFAILURES DESC NULLS LAST"

        query += " LIMIT 1000"

        # Execute query
        conn = get_snowflake_connection()
        cur = conn.cursor()

        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        # Fetch and process results
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()

        cur.close()
        conn.close()

        # Process results
        result = []
        for row in data:
            record = {}
            for i, col in enumerate(columns):
                value = row[i]

                if col in ['AVG_EXTRAFAILURES', 'TOTAL_EXTRAFAILURES']:
                    record[col] = clean_numeric_value(value)
                elif col in ['AVG_IDXCONTR', 'TOTAL_IDXCONTR']:
                    record[col] = clean_contribution_value(value)
                elif col in ['AVG_ACTUAL', 'AVG_TARGET']:
                    record[col] = clean_numeric_value(value)
                elif col == 'RECORD_COUNT':
                    record[col] = int(value) if value else 0
                elif col in ['EARLIEST_PERIOD', 'LATEST_PERIOD']:
                    if value is not None:
                        if isinstance(value, (datetime, pd.Timestamp)):
                            record[col] = value.isoformat()
                        else:
                            record[col] = None
                    else:
                        record[col] = None
                else:
                    record[col] = value

            # Add display fields
            if aggregate_all_metrics or record.get('METRICNAME') == 'ALL':
                record['METRIC_DISPLAY'] = 'All'
            elif record.get('METRICNAME') in metric_mapping:
                record['METRIC_DISPLAY'] = metric_mapping[record['METRICNAME']]
            else:
                record['METRIC_DISPLAY'] = record.get('METRICNAME', '')

            record['EXTRAFAILURES'] = record.get('AVG_EXTRAFAILURES', 0)
            record['IDXCONTR'] = record.get('AVG_IDXCONTR', 0)

            result.append(record)

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

        # Get date range for last 2 days
        end_date = datetime.now().strftime('%Y-%m-%d 23:59:59')
        start_date = (datetime.now() - timedelta(days=2)
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

        # Group by date and metric
        query += """
            GROUP BY USID, METRICNAME, DATE(PERIODSTART)
            ORDER BY DATE(PERIODSTART), METRICNAME
        """

        # Execute query
        conn = get_snowflake_connection()
        cur = conn.cursor()

        cur.execute(query, params)

        # Fetch results
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()

        cur.close()
        conn.close()

        # Process results
        result = []
        for row in data:
            record = {}
            for i, col in enumerate(columns):
                value = row[i]

                if col == 'EXTRAFAILURES':
                    record[col] = clean_numeric_value(value)
                elif col == 'DATE':
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
        return jsonify({'error': str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    print("🚀 Starting CQI Dashboard API Server (with CSV-based Submarket-Cluster Mapping)...")
    print(f"📄 Looking for mapping file: {MAPPING_CSV_PATH}")

    # Check if mapping file exists
    if os.path.exists(MAPPING_CSV_PATH):
        print(f"✅ Mapping file found: {MAPPING_CSV_PATH}")
        # Try to load and display summary
        mapping = load_submarket_cluster_mapping()
        if mapping:
            print(f"📊 Loaded mappings for {len(mapping)} submarkets")
            print(
                f"   Total cluster mappings: {sum(len(clusters) for clusters in mapping.values())}")
    else:
        print(
            f"⚠️  Mapping file not found. A sample file will be created at: {MAPPING_CSV_PATH}")
        print("   Please update it with your actual submarket-cluster mappings")

    # Check configuration
    if validate_config():
        print("✅ Configuration loaded from environment variables")
        print(
            f"📊 Connecting to Snowflake as user: {SNOWFLAKE_CONFIG.get('user')}")
    else:
        print("❌ Configuration incomplete. Please check environment variables.")
        print("\nRequired environment variables:")
        print("  SNOWFLAKE_USER")
        print("  SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
        print("\nOptional environment variables:")
        print("  SNOWFLAKE_ACCOUNT (default: nsasprd.east-us-2.privatelink)")
        print("  SNOWFLAKE_WAREHOUSE (default: USR_REPORTING_WH)")
        print("  SNOWFLAKE_DATABASE (default: PRD_MOBILITY)")
        print("  SNOWFLAKE_SCHEMA (default: PRD_MOBILITYSCORECARD_VIEWS)")
        print("  SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (if key is encrypted)")

    print("\n📡 API will be available at: http://localhost:5000")
    print("✨ NEW FEATURE: CSV-based Submarket-Cluster filtering!")
    print("-" * 50)

    app.run(debug=False, host='0.0.0.0', port=5000)
