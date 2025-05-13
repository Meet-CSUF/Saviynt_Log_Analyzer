import streamlit as st
import pandas as pd
import requests
import yaml
import logging
import time
import json
import sqlite3
from datetime import datetime
from analyzer.visualizer import Visualizer
from analyzer.data_manager import export_to_excel, get_analysis_data, init_db
from retrying import retry
import os

# Configure logging
logging.basicConfig(
    filename='log_analyzer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Backend API base URL
BACKEND_URL = "http://localhost:8000"

def load_config():
    """Load configuration from YAML file."""
    try:
        with open('config/config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error("Config file config/config.yaml not found")
        st.error("Configuration file not found. Please create config/config.yaml.")
        return {'app': {'log_levels': [], 'data_dir': 'data', 'state_dir': 'data'}}
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        st.error(f"Error loading config: {str(e)}")
        return {'app': {'log_levels': []}}

def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'selected_job_id' not in st.session_state:
        st.session_state.selected_job_id = None
    if 'dashboard_data' not in st.session_state:
        st.session_state.dashboard_data = None
    if 'notifications' not in st.session_state:
        st.session_state.notifications = []
    if 'csv_notifications' not in st.session_state:
        st.session_state.csv_notifications = []
    if 'backend_available' not in st.session_state:
        st.session_state.backend_available = False
    if 'db_initialized' not in st.session_state:
        st.session_state.db_initialized = False
    if 'log_viewer_job_id' not in st.session_state:
        st.session_state.log_viewer_job_id = None
    if 'cached_job_id' not in st.session_state:
        st.session_state.cached_job_id = None
    if 'show_dashboard' not in st.session_state:
        st.session_state.show_dashboard = False
    if 'last_notification_clear' not in st.session_state:
        st.session_state.last_notification_clear = time.time()
    if 'log_viewer_current_page' not in st.session_state:
        st.session_state.log_viewer_current_page = 1
    if 'log_viewer_total_pages' not in st.session_state:
        st.session_state.log_viewer_total_pages = 1
    if 'log_viewer_logs' not in st.session_state:
        st.session_state.log_viewer_logs = []
    if 'log_viewer_total_logs' not in st.session_state:
        st.session_state.log_viewer_total_logs = 0
    if 'log_viewer_last_job_id' not in st.session_state:
        st.session_state.log_viewer_last_job_id = None

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def check_backend_health():
    """Check if backend is running and fetch job status."""
    try:
        logger.debug(f"Attempting health check to {BACKEND_URL}/health")
        response = requests.get(f"{BACKEND_URL}/health", timeout=10)
        response.raise_for_status()
        st.session_state.backend_available = True
        logger.info("Backend health check passed")
        
        if st.session_state.selected_job_id:
            # Verify job_id exists in jobs table
            try:
                conn = sqlite3.connect('data/logs.db', timeout=30)
                cursor = conn.cursor()
                cursor.execute("SELECT job_id FROM jobs WHERE job_id = ?", (st.session_state.selected_job_id,))
                job_exists = cursor.fetchone()
                conn.close()
                
                if not job_exists:
                    logger.warning(f"Selected job_id {st.session_state.selected_job_id} not found in database")
                    st.session_state.notifications.append({
                        'type': 'warning',
                        'message': f"Selected job {st.session_state.selected_job_id} no longer exists. Please select a valid job.",
                        'timestamp': time.time()
                    })
                    st.session_state.selected_job_id = None
                    st.session_state.show_dashboard = False
                    return True
            except sqlite3.OperationalError as e:
                logger.error(f"Database error checking job_id {st.session_state.selected_job_id}: {str(e)}")
                st.session_state.notifications.append({
                    'type': 'error',
                    'message': f"Database error checking job status: {str(e)}",
                    'timestamp': time.time()
                })
                return True
            
            # Fetch job status
            try:
                job_response = requests.get(f"{BACKEND_URL}/jobs/{st.session_state.selected_job_id}/status", timeout=10)
                job_response.raise_for_status()
                job_data = job_response.json()
                files_processed = job_data.get('files_processed', 0)
                total_files = job_data.get('total_files', 0)
                logger.info(f"Backend status for job {st.session_state.selected_job_id}: {files_processed}/{total_files} files processed")
                st.session_state.notifications.append({
                    'type': 'success',
                    'message': f"Backend is healthy! Job {st.session_state.selected_job_id}: {files_processed}/{total_files} files processed.",
                    'timestamp': time.time()
                })
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch job status: {str(e)}")
                st.session_state.notifications.append({
                    'type': 'warning',
                    'message': f"Backend is healthy, but failed to fetch job status: {str(e)}",
                    'timestamp': time.time()
                })
        else:
            st.session_state.notifications.append({
                'type': 'success',
                'message': "Backend is healthy! No job selected.",
                'timestamp': time.time()
            })
        return True
    except requests.RequestException as e:
        logger.warning(f"Backend health check failed: {str(e)}")
        st.session_state.backend_available = False
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend is not responding. Job control actions are unavailable.",
            'timestamp': time.time()
        })
        return False

def apply_custom_css():
    """Apply Tailwind CSS with glassmorphism."""
    st.markdown(
        """
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
        <style>
        body {
            background: linear-gradient(to bottom, #f0ede6, #e5e2db);
            font-family: 'Inter', sans-serif;
            color: #1F2937;
        }
        .header {
            background: linear-gradient(90deg, #12133f, #2A2B5A);
            color: #FFFFFF;
            padding: 3rem 2rem;
            border-radius: 16px;
            text-align: center;
            margin-bottom: 2rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
            position: relative;
            overflow: hidden;
        }
        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
            animation: pulse 4s infinite;
        }
        .header h1 {
            font-size: 3rem;
            font-weight: 700;
            margin: 0;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
        }
        .header p {
            font-size: 1.25rem;
            opacity: 0.9;
        }
        .card {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 2rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(209, 213, 219, 0.3);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .card:hover {
            transform: translateY(-8px);
            box-shadow: 0 12px 24px rgba(0, 0, 0, 0.15);
        }
        .stButton>button {
            background: linear-gradient(90deg, #12133f, #2A2B5A);
            color: #FFFFFF;
            border: none;
            border-radius: 12px;
            padding: 0.75rem 2rem;
            font-size: 1.1rem;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s, background 0.3s, box-shadow 0.3s;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }
        .stButton>button:hover {
            background: linear-gradient(90deg, #2A2B5A, #12133f);
            transform: scale(1.05);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.3);
        }
        .sidebar .stButton>button {
            width: 100%;
            margin-bottom: 1rem;
        }
        .stTextInput>div>input {
            border-radius: 12px;
            border: 1px solid #D1D5DB;
            padding: 0.75rem;
            background: rgba(255, 255, 255, 0.9);
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        .stTextInput>div>input:focus {
            border-color: #12133f;
            box-shadow: 0 0 0 3px rgba(18, 19, 63, 0.1);
        }
        .stSelectbox>div>select {
            border-radius: 12px;
            border: 1px solid #D1D5DB;
            padding: 0.75rem;
            background: rgba(255, 255, 255, 0.9);
        }
        .notification-success {
            background: rgba(209, 250, 229, 0.95);
            color: #065F46;
            padding: 1rem;
            border-radius: 12px;
            margin-bottom: 1rem;
            border: 1px solid #34D399;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            animation: slideIn 0.3s ease;
        }
        .notification-error {
            background: rgba(254, 226, 226, 0.95);
            color: #991B1B;
            padding: 1rem;
            border-radius: 12px;
            margin-bottom: 1rem;
            border: 1px solid #F87171;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            animation: slideIn 0.3s ease;
        }
        .notification-warning {
            background: rgba(254, 243, 199, 0.95);
            color: #92400E;
            padding: 1rem;
            border-radius: 12px;
            margin-bottom: 1rem;
            border: 1px solid #FBBF24;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            animation: slideIn 0.3s ease;
        }
        .sidebar-content {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 2rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(209, 213, 219, 0.3);
        }
        .tab-content {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 2.5rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(209, 213, 219, 0.3);
        }
        .tooltip {
            position: relative;
            display: inline-block;
        }
        .tooltip .tooltiptext {
            visibility: hidden;
            width: 200px;
            background: #2A2B5A;
            color: #FFFFFF;
            text-align: center;
            border-radius: 8px;
            padding: 0.75rem;
            position: absolute;
            z-index: 1;
            bottom: 125%;
            left: 50%;
            margin-left: -100px;
            opacity: 0;
            transition: opacity 0.3s;
        }
        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
        @keyframes slideIn {
            from { transform: translateX(-20px); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        @keyframes pulse {
            0% { transform: scale(1); opacity: 0.5; }
            50% { transform: scale(1.2); opacity: 0.3; }
            100% { transform: scale(1); opacity: 0.5; }
        }
        </style>
        """,
        unsafe_allow_html=True
    )

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_job_status():
    """Fetch all job statuses from SQLite database."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        query = """
            SELECT job_id, folder_path, status, files_processed, total_files, start_time, last_updated
            FROM jobs
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching job status: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error fetching job status: {str(e)}",
            'timestamp': time.time()
        })
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching job status: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching job status: {str(e)}",
            'timestamp': time.time()
        })
        return pd.DataFrame()

@st.cache_data
def get_job_metadata(job_id: str):
    """Fetch unique classes and services for a job from job_metadata table, cached."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        classes = pd.read_sql_query(
            "SELECT value FROM job_metadata WHERE job_id = ? AND type = 'class'",
            conn,
            params=[job_id]
        )['value'].dropna().unique().tolist()
        services = pd.read_sql_query(
            "SELECT value FROM job_metadata WHERE job_id = ? AND type = 'service'",
            conn,
            params=[job_id]
        )['value'].dropna().unique().tolist()
        conn.close()
        logger.info(f"Fetched metadata for job_id: {job_id}, classes: {len(classes)}, services: {len(services)}")
        return classes, services
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching metadata for job_id {job_id}: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error fetching metadata: {str(e)}",
            'timestamp': time.time()
        })
        return [], []
    except Exception as e:
        logger.error(f"Error fetching metadata for job_id {job_id}: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching metadata: {str(e)}",
            'timestamp': time.time()
        })
        return [], []

@st.cache_data
def get_logs_by_class_and_level(job_id: str, class_name: str, level: str, page: int, logs_per_page: int, search_query: str = None, use_regex: bool = False):
    """Retrieve logs by class and level from SQLite, cached."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        offset = (page - 1) * logs_per_page
        query = """
            SELECT timestamp, log_message, level, class
            FROM logs
            WHERE job_id = ? AND class = ? AND level = ?
        """
        params = [job_id, class_name, level]
        
        if search_query:
            if use_regex:
                query += " AND log_message REGEXP ?"
                params.append(search_query)
            else:
                query += " AND log_message LIKE ?"
                params.append(f'%{search_query}%')
        
        query += " LIMIT ? OFFSET ?"
        params.extend([logs_per_page, offset])
        
        logs_df = pd.read_sql_query(query, conn, params=params)
        count_query = """
            SELECT COUNT(*) as total
            FROM logs
            WHERE job_id = ? AND class = ? AND level = ?
        """
        count_params = [job_id, class_name, level]
        
        if search_query:
            if use_regex:
                count_query += " AND log_message REGEXP ?"
                count_params.append(search_query)
            else:
                count_query += " AND log_message LIKE ?"
                count_params.append(f'%{search_query}%')
        
        total_logs = pd.read_sql_query(count_query, conn, params=count_params)['total'].iloc[0]
        conn.close()
        logger.debug(f"Fetched {len(logs_df)} logs, total_logs={total_logs}, page={page}")
        return logs_df.to_dict('records'), total_logs
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching logs by class and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error: {str(e)}",
            'timestamp': time.time()
        })
        raise
    except Exception as e:
        logger.error(f"Error fetching logs by class and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching logs: {str(e)}",
            'timestamp': time.time()
        })
        raise

@st.cache_data
def get_logs_by_service_and_level(job_id: str, service_name: str, level: str, page: int, logs_per_page: int, search_query: str = None, use_regex: bool = False):
    """Retrieve logs by service and level from SQLite, cached."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        offset = (page - 1) * logs_per_page
        query = """
            SELECT timestamp, log_message, level, service
            FROM logs
            WHERE job_id = ? AND service = ? AND level = ?
        """
        params = [job_id, service_name, level]
        
        if search_query:
            if use_regex:
                query += " AND log_message REGEXP ?"
                params.append(search_query)
            else:
                query += " AND log_message LIKE ?"
                params.append(f'%{search_query}%')
        
        query += " LIMIT ? OFFSET ?"
        params.extend([logs_per_page, offset])
        
        logs_df = pd.read_sql_query(query, conn, params=params)
        count_query = """
            SELECT COUNT(*) as total
            FROM logs
            WHERE job_id = ? AND service = ? AND level = ?
        """
        count_params = [job_id, service_name, level]
        
        if search_query:
            if use_regex:
                count_query += " AND log_message REGEXP ?"
                count_params.append(search_query)
            else:
                count_query += " AND log_message LIKE ?"
                count_params.append(f'%{search_query}%')
        
        total_logs = pd.read_sql_query(count_query, conn, params=count_params)['total'].iloc[0]
        conn.close()
        logger.debug(f"Fetched {len(logs_df)} logs, total_logs={total_logs}, page={page}")
        return logs_df.to_dict('records'), total_logs
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching logs by service and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error: {str(e)}",
            'timestamp': time.time()
        })
        raise
    except Exception as e:
        logger.error(f"Error fetching logs by service and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching logs: {str(e)}",
            'timestamp': time.time()
        })
        raise

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def start_analysis(folder_path):
    """Start a new analysis job via backend API."""
    if not st.session_state.backend_available:
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend server is not running. Please start `python backend.py`.",
            'timestamp': time.time()
        })
        return
    try:
        response = requests.post(f"{BACKEND_URL}/jobs/start", json={"folder_path": folder_path}, timeout=10)
        response.raise_for_status()
        job = response.json()
        st.session_state.selected_job_id = job['job_id']
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Started analysis job: {job['job_id']}",
            'timestamp': time.time()
        })
        logger.info(f"Started analysis for folder: {folder_path}, job_id: {job['job_id']}")
    except requests.RequestException as e:
        logger.error(f"Error starting analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error starting analysis: {str(e)}",
            'timestamp': time.time()
        })

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def pause_analysis(job_id):
    """Pause an analysis job via backend API."""
    if not st.session_state.backend_available:
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend server is not running. Please start `python backend.py`.",
            'timestamp': time.time()
        })
        return
    try:
        response = requests.post(f"{BACKEND_URL}/jobs/{job_id}/pause", timeout=10)
        response.raise_for_status()
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Paused analysis job: {job_id}",
            'timestamp': time.time()
        })
        logger.info(f"Paused analysis job: {job_id}")
    except requests.RequestException as e:
        logger.error(f"Error pausing analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error pausing analysis: {str(e)}",
            'timestamp': time.time()
        })

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def resume_analysis(job_id):
    """Resume a paused analysis job via backend API."""
    if not st.session_state.backend_available:
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend server is not running. Please start `python backend.py`.",
            'timestamp': time.time()
        })
        return
    try:
        response = requests.post(f"{BACKEND_URL}/jobs/{job_id}/resume", timeout=10)
        response.raise_for_status()
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Resumed analysis job: {job_id}",
            'timestamp': time.time()
        })
        logger.info(f"Resumed analysis job: {job_id}")
    except requests.RequestException as e:
        logger.error(f"Error resuming analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error resuming analysis: {str(e)}",
            'timestamp': time.time()
        })

def view_analysis(visualizer):
    """View analysis results for the selected job with progress feedback in main page."""
    try:
        if not st.session_state.selected_job_id:
            st.session_state.notifications.append({
                'type': 'warning',
                'message': "Please select a job to view analysis",
                'timestamp': time.time()
            })
            return
        
        with st.container():
            st.markdown('<div class="card">', unsafe_allow_html=True)
            with st.spinner("Loading analysis data..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                steps = 4
                step_increment = 1.0 / steps
                
                status_text.text("Fetching timeline data...")
                timeline_data = get_analysis_data(job_id=st.session_state.selected_job_id, query_type='timeline')
                # Sort timeline data by hour
                if not timeline_data.empty:
                    timeline_data['hour'] = pd.to_datetime(timeline_data['hour'])
                    timeline_data = timeline_data.sort_values('hour')
                progress_bar.progress(0.25)
                
                status_text.text("Fetching class-level counts...")
                level_counts_by_class = get_analysis_data(job_id=st.session_state.selected_job_id, query_type='class')
                # Pivot class data: class as index, levels as columns
                if not level_counts_by_class.empty:
                    class_pivot = level_counts_by_class.pivot(index='class', columns='level', values='count').fillna(0)
                    # Ensure all log levels are present as columns
                    config = load_config()
                    log_levels = config['app']['log_levels']
                    for level in log_levels:
                        if level not in class_pivot.columns:
                            class_pivot[level] = 0
                    class_pivot = class_pivot.reset_index()
                else:
                    class_pivot = pd.DataFrame(columns=['class'] + log_levels)
                progress_bar.progress(0.50)
                
                status_text.text("Fetching service-level counts...")
                level_counts_by_service = get_analysis_data(job_id=st.session_state.selected_job_id, query_type='service')
                # Pivot service data: service as index, levels as columns
                if not level_counts_by_service.empty:
                    service_pivot = level_counts_by_service.pivot(index='service', columns='level', values='count').fillna(0)
                    # Ensure all log levels are present as columns
                    for level in log_levels:
                        if level not in service_pivot.columns:
                            service_pivot[level] = 0
                    service_pivot = service_pivot.reset_index()
                else:
                    service_pivot = pd.DataFrame(columns=['service'] + log_levels)
                progress_bar.progress(0.75)
                
                status_text.text("Fetching class and service totals...")
                # Calculate total counts for class and service bar/pie charts
                class_totals = level_counts_by_class.groupby('class')['count'].sum().reset_index()
                service_totals = level_counts_by_service.groupby('service')['count'].sum().reset_index()
                progress_bar.progress(1.0)
                
                if all(df.empty for df in [timeline_data, level_counts_by_class, level_counts_by_service, class_totals, service_totals]):
                    st.session_state.notifications.append({
                        'type': 'warning',
                        'message': "No analysis data available for this job",
                        'timestamp': time.time()
                    })
                    progress_bar.empty()
                    status_text.empty()
                    st.markdown('</div>', unsafe_allow_html=True)
                    return
                
                st.session_state.dashboard_data = {
                    'timeline_data': timeline_data,
                    'class_pivot': class_pivot,
                    'service_pivot': service_pivot,
                    'class_totals': class_totals,
                    'service_totals': service_totals
                }
                
                st.session_state.show_dashboard = True
                
                st.session_state.notifications.append({
                    'type': 'success',
                    'message': "Analysis data loaded successfully",
                    'timestamp': time.time()
                })
                
                progress_bar.empty()
                status_text.empty()
            st.markdown('</div>', unsafe_allow_html=True)
            
    except Exception as e:
        logger.error(f"Error viewing analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error viewing analysis: {str(e)}",
            'timestamp': time.time()
        })

def download_results(job_id):
    """Download analysis results as Excel."""
    try:
        with st.spinner("Generating Excel file..."):
            excel_file = export_to_excel(job_id)
            with open(excel_file, 'rb') as f:
                st.download_button(
                    label="Download Excel",
                    data=f,
                    file_name=f"analysis_results_{job_id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            st.session_state.notifications.append({
                'type': 'success',
                'message': "Excel file generated successfully",
                'timestamp': time.time()
            })
    except FileNotFoundError:
        logger.error(f"Excel file not found for job_id: {job_id}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Excel file could not be generated",
            'timestamp': time.time()
        })
    except Exception as e:
        logger.error(f"Error downloading results: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error downloading results: {str(e)}",
            'timestamp': time.time()
        })

def process_csv_files(uploaded_files):
    """Process uploaded CSV files."""
    csv_data = {}
    for file in uploaded_files:
        try:
            df = pd.read_csv(file)
            file_name = file.name.lower().replace('.csv', '')
            if file_name in [
                'class_level_counts', 'level_summary', 'class_summary', 'pod_summary',
                'container_summary', 'host_summary', 'class_level_pod', 'hourly_level_counts',
                'thread_summary', 'error_analysis', 'time_range'
            ]:
                csv_data[file_name] = df
            else:
                st.session_state.csv_notifications.append({
                    'type': 'warning',
                    'message': f"Unsupported CSV file: {file.name}",
                    'timestamp': time.time()
                })
        except Exception as e:
            logger.error(f"Error processing CSV {file.name}: {str(e)}")
            st.session_state.csv_notifications.append({
                'type': 'error',
                'message': f"Error processing CSV {file.name}: {str(e)}",
                'timestamp': time.time()
            })
    return csv_data

def display_notifications():
    """Display notifications with 5-second auto-expiry."""
    current_time = time.time()
    
    # Clear expired notifications
    st.session_state.notifications = [n for n in st.session_state.notifications if current_time - n['timestamp'] < 5]
    
    # Render notifications in a single container
    notification_container = st.empty()
    with notification_container.container():
        for i, notification in enumerate(st.session_state.notifications):
            logger.debug(f"Displaying notification {i}: {notification['message']}")
            if notification['type'] == 'success':
                st.markdown(
                    f'<div class="notification-success" key="notification_{i}_{notification["timestamp"]}">✅ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'error':
                st.markdown(
                    f'<div class="notification-error" key="notification_{i}_{notification["timestamp"]}">❌ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'warning':
                st.markdown(
                    f'<div class="notification-warning" key="notification_{i}_{notification["timestamp"]}">⚠️ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
    
    # Force clear after 5 seconds
    if st.session_state.notifications and current_time - st.session_state.last_notification_clear >= 5:
        st.session_state.notifications = []
        st.session_state.last_notification_clear = current_time
        notification_container.empty()
        logger.debug("Cleared notification container eigenlijk")
        st.experimental_rerun()

def display_csv_notifications():
    """Display CSV-specific notifications with 5-second auto-expiry."""
    current_time = time.time()
    
    # Clear expired notifications
    st.session_state.csv_notifications = [n for n in st.session_state.csv_notifications if current_time - n['timestamp'] < 5]
    
    # Render notifications in a single container
    csv_notification_container = st.empty()
    with csv_notification_container.container():
        for i, notification in enumerate(st.session_state.csv_notifications):
            logger.debug(f"Displaying CSV notification {i}: {notification['message']}")
            if notification['type'] == 'success':
                st.markdown(
                    f'<div class="notification-success" key="csv_notification_{i}_{notification["timestamp"]}">✅ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'error':
                st.markdown(
                    f'<div class="notification-error" key="csv_notification_{i}_{notification["timestamp"]}">❌ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'warning':
                st.markdown(
                    f'<div class="notification-warning" key="csv_notification_{i}_{notification["timestamp"]}">⚠️ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
    
    # Force clear after 5 seconds
    if st.session_state.csv_notifications and current_time - st.session_state.last_notification_clear >= 5:
        st.session_state.csv_notifications = []
        st.session_state.last_notification_clear = current_time
        csv_notification_container.empty()
        logger.debug("Cleared CSV notification container")
        st.experimental_rerun()

def update_selected_job_id():
    """Update selected job ID in session state for Log Analysis tab."""
    selected_job = st.session_state.job_select
    if selected_job != 'Select a job...':
        st.session_state.selected_job_id = selected_job
        st.session_state.show_dashboard = False
    else:
        st.session_state.selected_job_id = None
        st.session_state.show_dashboard = False

def update_log_viewer_job_id():
    """Update selected job ID for Log Viewer tab and manage cache."""
    selected_job = st.session_state.log_viewer_job_select
    if selected_job != 'Select a job...':
        if st.session_state.log_viewer_job_id != selected_job:
            st.session_state.log_viewer_job_id = selected_job
            if st.session_state.log_viewer_last_job_id != selected_job:
                get_job_metadata.clear()
                get_logs_by_class_and_level.clear()
                get_logs_by_service_and_level.clear()
                st.session_state.cached_job_id = selected_job
                st.session_state.log_viewer_last_job_id = selected_job
                logger.info(f"Cleared cache for new job_id: {selected_job}")
            # Reset pagination
            st.session_state.log_viewer_current_page = 1
            st.session_state.log_viewer_total_pages = 1
            st.session_state.log_viewer_logs = []
            st.session_state.log_viewer_total_logs = 0
    else:
        st.session_state.log_viewer_job_id = None
        get_job_metadata.clear()
        get_logs_by_class_and_level.clear()
        get_logs_by_service_and_level.clear()
        st.session_state.cached_job_id = None
        st.session_state.log_viewer_last_job_id = None
        st.session_state.log_viewer_current_page = 1
        st.session_state.log_viewer_total_pages = 1
        st.session_state.log_viewer_logs = []
        st.session_state.log_viewer_total_logs = 0

def main():
    """Main Streamlit application."""
    st.set_page_config(page_title="Saviynt Log Analyzer", layout="wide", initial_sidebar_state="expanded")
    
    import os
    os.makedirs('data', exist_ok=True)
    initialize_session_state()
    
    if not st.session_state.db_initialized:
        try:
            init_db()
            st.session_state.db_initialized = True
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            st.error(f"Failed to initialize database: {str(e)}")
            return
    
    apply_custom_css()
    check_backend_health()

    st.markdown(
        """
        <div class="header">
            <h1>Saviynt Log Analyzer</h1>
            <p>Unleash the Power of Log Analytics with Unmatched Precision</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    tab1, tab2, tab3 = st.tabs(["📊 Log Analysis", "🔍 Log Viewer", "📈 CSV Visualization"])

    with tab1:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        st.header("Log Analysis")
        config = load_config()
        visualizer = Visualizer(config)

        job_status_df = get_job_status()
        job_options = ['Select a job...']
        if not job_status_df.empty and 'job_id' in job_status_df.columns:
            job_options += job_status_df['job_id'].tolist()
        else:
            st.info("No jobs available. Start a new analysis to create a job.")

        col1, col2 = st.columns([3, 1])
        with col1:
            st.selectbox(
                "Select JOBID",
                options=job_options,
                key="job_select",
                on_change=update_selected_job_id,
                help="Choose a job to view its analysis results"
            )
        with col2:
            st.markdown('<div class="tooltip">', unsafe_allow_html=True)
            if st.button("Clear Cache", key="clear_cache"):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.session_state.notifications.append({
                    'type': 'success',
                    'message': "Cache cleared successfully",
                    'timestamp': time.time()
                })
            st.markdown('<span class="tooltiptext">Clears cached data to refresh the application</span></div>', unsafe_allow_html=True)

        with st.container():
            if st.session_state.selected_job_id and not job_status_df.empty:
                job_info = job_status_df[job_status_df['job_id'] == st.session_state.selected_job_id].iloc[0]
                st.markdown(
                    f"""
                    <div class="card">
                        <h3 class="text-lg font-semibold text-gray-800">Job Details</h3>
                        <p><strong>Job ID:</strong> {st.session_state.selected_job_id}</p>
                        <p><strong>Folder Path:</strong> {job_info.get('folder_path', 'N/A')}</p>
                        <p><strong>Status:</strong> {job_info.get('status', 'N/A')}</p>
                        <p><strong>Files Processed:</strong> {job_info.get('files_processed', 0)} / {job_info.get('total_files', 0)}</p>
                        <p><strong>Start Time:</strong> {job_info.get('start_time', 'N/A')}</p>
                        <p><strong>Last Updated:</strong> {job_info.get('last_updated', 'N/A')}</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            if st.session_state.show_dashboard and st.session_state.dashboard_data:
                st.markdown('<div class="card">', unsafe_allow_html=True)
                visualizer.display_dashboard(
                    st.session_state.dashboard_data['timeline_data'],
                    st.session_state.dashboard_data['class_pivot'],
                    st.session_state.dashboard_data['service_pivot'],
                    st.session_state.dashboard_data['class_totals'],
                    st.session_state.dashboard_data['service_totals']
                )
                st.markdown('</div>', unsafe_allow_html=True)

        with st.sidebar:
            st.markdown('<div class="sidebar-content">', unsafe_allow_html=True)
            st.header("Analysis Controls")
            folder_path = st.text_input(
                "Log Folder Path",
                placeholder="e.g., data/customer_logs",
                key="folder_path",
                help="Enter the path to the folder containing .gz log files"
            )
            st.markdown('<div class="tooltip">', unsafe_allow_html=True)
            if st.button("Start Analysis", key="start_analysis"):
                if folder_path:
                    if st.session_state.backend_available:
                        start_analysis(folder_path)
                    else:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Cannot start analysis: Backend server is not running. Please start `python backend.py`.",
                            'timestamp': time.time()
                        })
                else:
                    st.session_state.notifications.append({
                        'type': 'error',
                        'message': "Please provide a log folder path",
                        'timestamp': time.time()
                    })
            st.markdown('<span class="tooltiptext">Starts a new analysis job for the specified folder</span></div>', unsafe_allow_html=True)

            if st.session_state.selected_job_id:
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("Pause Analysis", key="pause_analysis"):
                    if st.session_state.backend_available:
                        pause_analysis(st.session_state.selected_job_id)
                    else:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Cannot pause analysis: Backend server is not running. Please start `python backend.py`.",
                            'timestamp': time.time()
                        })
                st.markdown('<span class="tooltiptext">Pauses the selected analysis job</span></div>', unsafe_allow_html=True)
                
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("Resume Analysis", key="resume_analysis"):
                    if st.session_state.backend_available:
                        resume_analysis(st.session_state.selected_job_id)
                    else:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Cannot resume analysis: Backend server is not running. Please start `python backend.py`.",
                            'timestamp': time.time()
                        })
                st.markdown('<span class="tooltiptext">Resumes a paused analysis job</span></div>', unsafe_allow_html=True)
                
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("View Analysis", key="view_analysis"):
                    view_analysis(visualizer)
                st.markdown('<span class="tooltiptext">Displays analysis results for the selected job</span></div>', unsafe_allow_html=True)
                
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("Download Results", key="download_results"):
                    download_results(st.session_state.selected_job_id)
                st.markdown('<span class="tooltiptext">Downloads analysis results as an Excel file</span></div>', unsafe_allow_html=True)
            
            if st.button("Check Backend Status", key="check_backend_status"):
                check_backend_health()
            
            st.markdown('</div>', unsafe_allow_html=True)

        display_notifications()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        st.header("Log Viewer")
        
        job_status_df = get_job_status()
        job_options = ['Select a job...']
        if not job_status_df.empty and 'job_id' in job_status_df.columns:
            job_options += job_status_df['job_id'].tolist()
        else:
            st.info("No jobs available. Start a new analysis in the Log Analysis tab to create a job.")
        
        st.selectbox(
            "Select JOBID",
            options=job_options,
            key="log_viewer_job_select",
            on_change=update_log_viewer_job_id,
            help="Choose a job to view its logs"
        )

        if st.session_state.log_viewer_job_id:
            config = load_config()
            with st.spinner("Loading log viewer data..."):
                classes, services = get_job_metadata(st.session_state.log_viewer_job_id)
                class_options = ['None'] + classes if classes else ['None']
                service_options = ['None'] + services if services else ['None']
                
                log_level = st.selectbox(
                    "Select Log Level",
                    config['app']['log_levels'],
                    key="log_level_viewer",
                    help="Choose a log level to filter logs"
                )
                col1, col2 = st.columns(2)
                with col1:
                    selected_class = st.selectbox(
                        "Select Class",
                        class_options,
                        key="class_viewer",
                        help="Select a class to view its logs"
                    )
                with col2:
                    selected_service = st.selectbox(
                        "Select Service",
                        service_options,
                        key="service_viewer",
                        help="Select a service to view its logs"
                    )
                
                search_query = st.text_input(
                    "Search Logs",
                    placeholder="Enter search term",
                    key="search_viewer",
                    help="Search logs by message content"
                )
                use_regex = st.checkbox("Use Regex", key="regex_viewer", help="Enable regex for search queries")
                
                logs_per_page = 50
                page = st.number_input(
                    "Page",
                    min_value=1,
                    max_value=max(1, st.session_state.log_viewer_total_pages),
                    value=st.session_state.log_viewer_current_page,
                    step=1,
                    key="page_viewer",
                    help="Select page for paginated results"
                )
                
                if st.button("Fetch Logs", key="fetch_logs"):
                    if selected_class == 'None' and selected_service == 'None':
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Please select a class or service",
                            'timestamp': time.time()
                        })
                    elif log_level not in config['app']['log_levels']:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Please select a valid log level",
                            'timestamp': time.time()
                        })
                    else:
                        with st.spinner("Fetching logs..."):
                            try:
                                st.session_state.log_viewer_current_page = page
                                logs, total_logs = (get_logs_by_class_and_level if selected_class != 'None' else get_logs_by_service_and_level)(
                                    st.session_state.log_viewer_job_id,
                                    selected_class if selected_class != 'None' else selected_service,
                                    log_level,
                                    page,
                                    logs_per_page,
                                    search_query,
                                    use_regex
                                )
                                st.session_state.log_viewer_logs = logs
                                st.session_state.log_viewer_total_logs = total_logs
                                st.session_state.log_viewer_total_pages = max(1, (total_logs + logs_per_page - 1) // logs_per_page)
                                
                                if logs:
                                    st.dataframe(pd.DataFrame(logs), use_container_width=True)
                                    st.markdown(f"**Total Logs:** {total_logs} | **Page:** {page} of {st.session_state.log_viewer_total_pages}")
                                    st.download_button(
                                        label="Download Logs as JSON",
                                        data=json.dumps(logs, indent=2),
                                        file_name=f"{log_level}_logs_page_{page}.json",
                                        mime="application/json",
                                        key=f"download_viewer_{page}"
                                    )
                                    st.session_state.notifications.append({
                                        'type': 'success',
                                        'message': f"Loaded {len(logs)} logs for page {page}",
                                        'timestamp': time.time()
                                    })
                                else:
                                    st.info("No logs found for the selected criteria")
                                    st.session_state.log_viewer_logs = []
                                    st.session_state.log_viewer_total_logs = 0
                                    st.session_state.log_viewer_total_pages = 1
                            except Exception as e:
                                st.session_state.notifications.append({
                                    'type': 'error',
                                    'message': f"Failed to fetch logs: {str(e)}",
                                    'timestamp': time.time()
                                })
                                st.session_state.log_viewer_logs = []
                                st.session_state.log_viewer_total_logs = 0
                
                # Display current logs if available
                if st.session_state.log_viewer_logs:
                    st.dataframe(pd.DataFrame(st.session_state.log_viewer_logs), use_container_width=True)
                    st.markdown(f"**Total Logs:** {st.session_state.log_viewer_total_logs} | **Page:** {st.session_state.log_viewer_current_page} of {st.session_state.log_viewer_total_pages}")
                    st.download_button(
                        label="Download Logs as JSON",
                        data=json.dumps(st.session_state.log_viewer_logs, indent=2),
                        file_name=f"{log_level}_logs_page_{st.session_state.log_viewer_current_page}.json",
                        mime="application/json",
                        key=f"download_viewer_persistent_{st.session_state.log_viewer_current_page}"
                    )
        else:
            st.info("Please select a job to view logs")
        
        display_notifications()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab3:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        st.header("CSV Visualization")
        uploaded_files = st.file_uploader(
            "Upload CSV Files",
            accept_multiple_files=True,
            type=['csv'],
            help="Upload CSV files for visualization"
        )
        if uploaded_files:
            with st.spinner("Processing CSV files..."):
                csv_data = process_csv_files(uploaded_files)
                visualizer = Visualizer(load_config())
                visualizer.display_csv_dashboard(csv_data)
                display_csv_notifications()
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()