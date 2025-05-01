import streamlit as st
import os
import yaml
import logging
import re
from analyzer.log_processor import LogProcessor
from analyzer.data_manager import DataManager
from analyzer.visualizer import Visualizer
import pandas as pd
from datetime import datetime
import time
import sys
import glob

# Increase recursion limit
sys.setrecursionlimit(3000)

# Configure logging
logging.basicConfig(
    filename='log_analyzer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

def update_log_folder():
    """Callback to update st.session_state.log_folder when text input changes."""
    new_value = st.session_state.log_folder_input
    logger.debug(f"update_log_folder called, new_value: {new_value}")
    st.session_state.log_folder = new_value

def get_available_states():
    """Get list of saved state files sorted by datetime (newest first)."""
    state_dir = config['paths']['state_dir']
    state_files = glob.glob(os.path.join(state_dir, '*.json'))
    state_info = []
    for file in state_files:
        try:
            file_name = os.path.basename(file)
            parts = file_name.rsplit('_', 1)
            if len(parts) == 2:
                folder_path = parts[0].replace('_', '/')
                dt = datetime.strptime(parts[1].replace('.json', ''), '%Y%m%d%H%M%S')
                state_info.append((file, folder_path, dt))
        except Exception as e:
            logger.warning(f"Invalid state file name {file}: {str(e)}")
    state_info.sort(key=lambda x: x[2], reverse=True)
    return state_info

def render_full_ui():
    """Render the entire UI within a single container for log analysis."""
    with st.container():
        st.markdown('<div class="main-content">', unsafe_allow_html=True)

        # Notifications
        for notification in st.session_state.notifications:
            if notification['type'] == 'success':
                st.success(notification['message'])
            elif notification['type'] == 'error':
                st.error(notification['message'])
            elif notification['type'] == 'warning':
                st.warning(notification['message'])
        st.session_state.notifications = [
            n for n in st.session_state.notifications
            if time.time() - n['timestamp'] < 5
        ]

        # Progress
        progress = st.session_state.files_processed / max(st.session_state.total_files, 1) if st.session_state.total_files > 0 else 0
        st.progress(min(progress, 1.0))

        # Status
        status_text = (
            f"Processing file {st.session_state.files_processed}/{st.session_state.total_files}"
            if st.session_state.app_state == 'RUNNING'
            else "Analysis completed" if st.session_state.files_processed == st.session_state.total_files and st.session_state.files_processed > 0
            else f"Processed {st.session_state.files_processed}/{st.session_state.total_files} files"
        )
        st.markdown(f'<p class="status-message">{status_text}</p>', unsafe_allow_html=True)

        # Dashboard
        if not all(df.empty for df in st.session_state.dashboard_data.values()):
            st.session_state.visualizer.display_dashboard(
                st.session_state.dashboard_data['level_counts_by_class'],
                st.session_state.dashboard_data['level_counts_by_service'],
                st.session_state.dashboard_data['timeline_data'],
                st.session_state.dashboard_data['class_service_counts'],
                log_processor=st.session_state.processor
            )

        # Footer
        days_processed = len(set(folder.split('-')[0] for folder in st.session_state.folders_processed)) if st.session_state.folders_processed else 0
        hours_processed = len(st.session_state.folders_processed) if st.session_state.folders_processed else 0
        eta_text = "N/A"
        if st.session_state.files_processed > 0 and st.session_state.average_file_time:
            remaining_files = st.session_state.files_remaining
            eta_seconds = remaining_files * st.session_state.average_file_time
            if eta_seconds < 60:
                eta_text = f"{int(eta_seconds)} seconds"
            elif eta_seconds < 3600:
                eta_text = f"{int(eta_seconds // 60)} minutes"
            else:
                eta_text = f"{int(eta_seconds // 3600)} hours {int((eta_seconds % 3600) // 60)} minutes"
        st.markdown(f"""
        <div class="footer">
        <div class="counter">
            <span>{st.session_state.total_lines_processed}</span>
            <p>Lines Processed</p>
        </div>
        <div class="counter">
            <span>{st.session_state.files_processed}</span>
            <p>Files Processed</p>
        </div>
        <div class="counter">
            <span>{st.session_state.files_remaining}</span>
            <p>Files Remaining</p>
        </div>
        <div class="counter">
            <span>{hours_processed}</span>
            <p>Hours Processed</p>
        </div>
        <div class="counter">
            <span>{days_processed}</span>
            <p>Days Processed</p>
        </div>
        <div class="counter">
            <span>{st.session_state.current_folder or 'None'}</span>
            <p>Current Folder</p>
        </div>
        <div class="counter">
            <span>{st.session_state.current_file or 'None'}</span>
            <p>Current File</p>
        </div>
        <div class="counter">
            <span>{eta_text}</span>
            <p>Est. Time to Complete</p>
        </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

def main():
    st.set_page_config(page_title="Saviynt Log Analyzer", page_icon="static/saviynt_favicon.ico", layout="wide")

    # Custom CSS
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@600&family=Inter:wght@400;500&display=swap');
    @import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css');

    /* Global styles */
    .main {
        background: #f0ede6;
        color: #1A1A1A;
        font-family: 'Inter', sans-serif;
        min-height: 100vh;
        padding-bottom: 80px;
    }
    h1, h2, h3 {
        font-family: 'Poppins', sans-serif;
        color: #12133f;
    }

    /* Header */
    .header {
        display: flex;
        align-items: center;
        justify-content: flex-start;
        background: linear-gradient(90deg, #12133f, #2A2B5A);
        padding: 0 15px;
        height: 80px;
        border-bottom: 1px solid #D1D5DB;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    .header img {
        width: 50px;
        height: 50px;
        margin: 5px;
        border-radius: 8px;
        object-fit: cover;
    }
    .header .logo-placeholder {
        width: 50px;
        height: 50px;
        background: #12133f;
        margin: 5px;
        border-radius: 8px;
    }
    .header h1 {
        margin: 0;
        padding: 0;
        color: #FFFFFF;
        font-size: 1.6em;
        line-height: 1;
    }

    /* Sidebar */
    .css-1d391kg {
        background: #EDEFF5;
        padding: 20px;
        width: 300px !important;
        border-right: 1px solid #D1D5DB;
        box-shadow: 2px 0 4px rgba(0,0,0,0.1);
    }
    .css-1d391kg h2 {
        color: #12133f;
        font-size: 1.5em;
        text-align: center;
        margin-bottom: 20px;
    }
    .stButton>button {
        background: #12133f;
        color: #FFFFFF;
        border: none;
        border-radius: 8px;
        padding: 10px;
        width: 100%;
        font-size: 1em;
        font-family: 'Inter', sans-serif;
        transition: transform 0.2s, background 0.2s;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .stButton>button:hover {
        transform: scale(1.05);
        background: #2A2B5A;
    }
    .stButton>button:hover .button-icon {
        color: #FFFFFF;
    }
    .button-container {
        display: flex;
        align-items: center;
        margin-bottom: 10px;
    }
    .button-icon {
        margin-right: 8px;
        font-size: 1em;
        color: #12133f;
    }
    .stTextInput>label {
        color: #1A1A1A;
        font-size: 1em;
        font-weight: 500;
    }
    .stTextInput>div>input {
        background: #f0ede6;
        color: #1A1A1A;
        border: 1px solid #D1D5DB;
        border-radius: 8px;
        padding: 8px;
    }
    .stSelectbox>label {
        color: #1A1A1A;
        font-size: 1em;
        font-weight: 500;
    }
    .stSelectbox>div>select {
        background: #f0ede6;
        color: #1A1A1A;
        border: 1px solid #D1D5DB;
        border-radius: 8px;
        padding: 8px;
    }

    /* Main content */
    .main-content {
        max-width: 1200px;
        margin: 0 auto;
        padding: 20px;
    }

    /* Cards for plots */
    .card {
        background: #f0ede6;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    .stPlotlyChart, .stDataFrame {
        background: #f0ede6;
        border-radius: 8px;
        overflow: hidden;
    }
    .stDataFrame table {
        width: 100%;
        border-collapse: collapse;
        background: #f0ede6;
    }
    .stDataFrame th {
        position: sticky;
        top: 0;
        background: #F7F9FC;
        color: #1A1A1A;
        z-index: 10;
    }

    /* Expander */
    .stExpander {
        border: 1px solid #D1D5DB;
        border-radius: 8px;
    }
    .stExpander summary {
        background: #F7F9FC;
        color: #12133f;
        font-weight: 500;
    }

    /* Footer counters */
    .footer {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        background: #F7F9FC;
        padding: 10px 20px;
        display: flex;
        justify-content: space-around;
        border-top: 1px solid #D1D5DB;
        box-shadow: 0 -2px 4px rgba(0,0,0,0.1);
        z-index: 1000;
        height: 60px;
        align-items: center;
    }
    .counter {
        text-align: center;
        color: #12133f;
    }
    .counter span {
        font-size: 1em;
        color: #12133f;
        font-weight: bold;
        transition: all 0.5s ease-in-out;
        margin: 5px 0;
    }
    .counter p {
        font-size: 0.7em;
        color: #12133f;
        margin: 5px 0;
    }

    /* Progress bar */
    .stProgress .st-bo {
        background: #12133f;
    }
    .progress-container p {
        font-size: 1em;
        color: #12133f;
        margin-top: 5px;
        text-align: center;
    }
    .progress-container span {
        font-weight: 500;
        transition: all 0.5s ease-in-out;
    }

    /* Alerts */
    .stAlert {
        background: #FFF5F5;
        border: 1px solid #F56565;
        border-radius: 8px;
        color: #1A1A1A;
        margin-bottom: 10px;
        padding: 10px;
    }

    /* Status message */
    .status-message {
        font-size: 1em;
        color: #12133f;
        text-align: center;
        margin: 10px 0;
    }

    /* File uploader */
    .stFileUploader>label {
        color: #1A1A1A;
        font-size: 1em;
        font-weight: 500;
    }
    .stFileUploader>div {
        background: #f0ede6;
        border: 1px solid #D1D5DB;
        border-radius: 8px;
        padding: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

    # Header with logo
    logo_path = os.path.join('public', 'logo.png')
    if os.path.exists(logo_path):
        st.markdown(f'<div class="header"><img src="/public/logo.png" alt="Logo"><h1>Saviynt Log Analyzer</h1></div>', unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="header">
        <h1>Saviynt Log Analyzer</h1>
        </div>
        """, unsafe_allow_html=True)
        logger.warning("Logo file 'public/logo.png' not found. Using placeholder.")

    # Initialize session state
    if 'app_state' not in st.session_state:
        st.session_state.app_state = 'IDLE'
        st.session_state.processor = None
        st.session_state.data_manager = DataManager(config)
        st.session_state.visualizer = Visualizer(config)
        st.session_state.current_file = None
        st.session_state.current_line = 0
        st.session_state.total_lines_processed = 0
        st.session_state.files_processed = 0
        st.session_state.files_remaining = 0
        st.session_state.total_files = 0
        st.session_state.folders_processed = set()
        st.session_state.current_folder = None
        st.session_state.last_update_time = time.time()
        st.session_state.update_interval = config['app'].get('update_interval', 30)
        st.session_state.dashboard_data = {
            'level_counts_by_class': pd.DataFrame(),
            'level_counts_by_service': pd.DataFrame(),
            'timeline_data': pd.DataFrame(),
            'class_service_counts': pd.DataFrame()
        }
        st.session_state.notifications = []
        st.session_state.log_folder = ''
        st.session_state.logs_to_display_class = []
        st.session_state.logs_to_display_service = []
        st.session_state.total_logs_class = 0
        st.session_state.total_logs_service = 0
        st.session_state.log_page = 1
        st.session_state.selected_index = None
        st.session_state.selected_level = None
        st.session_state.average_file_time = 0
        st.session_state.uploaded_csv_data = {}  # New state for uploaded CSV data

    # Create tabs for Log Analysis and CSV Visualization
    tab1, tab2 = st.tabs(["Log Analysis", "CSV Visualization"])

    with tab1:
        # Sidebar controls for Log Analysis
        with st.sidebar:
            st.markdown("<h2>Analysis Controls</h2>", unsafe_allow_html=True)
            log_folder = st.text_input(
                "Log Folder Path",
                value=st.session_state.get('log_folder', ''),
                placeholder="e.g., /data/customer_logs",
                key="log_folder_input",
                on_change=update_log_folder
            )
            logger.debug(f"Log folder input after rendering: {log_folder}")

            state_info = get_available_states()
            state_options = [f"{folder_path} ({dt.strftime('%Y-%m-%d %H:%M:%S')})" for _, folder_path, dt in state_info]
            state_options.insert(0, "Select a state (default: latest)")
            selected_state = st.selectbox(
                "Select Saved State",
                options=state_options,
                key="state_select"
            )

            col1, col2 = st.columns(2)
            with col1:
                st.markdown('<div class="button-container"><i class="fas fa-play button-icon"></i>Start Analysis</div>', unsafe_allow_html=True)
                start_btn = st.button("Start Analysis", key="start_btn")
            with col2:
                st.markdown('<div class="button-container"><i class="fas fa-pause button-icon"></i>Pause Analysis</div>', unsafe_allow_html=True)
                pause_btn = st.button("Pause Analysis", key="pause_btn")

            st.markdown('<div class="button-container"><i class="fas fa-download button-icon"></i>Download Results</div>', unsafe_allow_html=True)
            download_btn = st.button("Download Results", key="download_btn")

            st.markdown('<div class="button-container"><i class="fas fa-forward button-icon"></i>Resume Analysis</div>', unsafe_allow_html=True)
            resume_btn = st.button("Resume Analysis", key="resume_btn")

        # Handle button actions
        if start_btn:
            logger.debug(f"Start Analysis clicked, log_folder: {st.session_state.log_folder}, current_state: {st.session_state.app_state}")
            if st.session_state.app_state == 'RUNNING':
                st.session_state.notifications.append({'type': 'warning', 'message': "Analysis is already running.", 'timestamp': time.time()})
                logger.warning("Start Analysis clicked while already RUNNING")
            elif not st.session_state.log_folder:
                st.session_state.notifications.append({'type': 'error', 'message': "Please enter a log folder path.", 'timestamp': time.time()})
                logger.warning("Start Analysis clicked without log folder path")
            elif not os.path.exists(st.session_state.log_folder):
                st.session_state.notifications.append({'type': 'error', 'message': "Log folder does not exist.", 'timestamp': time.time()})
                logger.error(f"Log folder does not exist: {st.session_state.log_folder}")
            elif not os.path.isdir(st.session_state.log_folder):
                st.session_state.notifications.append({'type': 'error', 'message': "Path is not a directory.", 'timestamp': time.time()})
                logger.error(f"Path is not a directory: {st.session_state.log_folder}")
            else:
                try:
                    all_files = []
                    for dirpath, dirnames, filenames in os.walk(st.session_state.log_folder):
                        logger.debug(f"Found folder: {dirpath}, subfolders: {dirnames}, files: {filenames}")
                        all_files.extend(os.path.join(dirpath, f) for f in filenames)
                    gz_files = [f for f in all_files if f.endswith('.gz')]
                    logger.debug(f"All files: {all_files}")
                    logger.debug(f"Filtered .gz files: {gz_files}")

                    if not gz_files:
                        st.session_state.notifications.append({'type': 'error', 'message': "No .gz files found in the specified folder.", 'timestamp': time.time()})
                        logger.error(f"No .gz files found in: {st.session_state.log_folder}")
                        st.session_state.app_state = 'IDLE'
                    else:
                        st.session_state.data_manager.clear_data()
                        if os.path.exists('data/state'):
                            for f in os.listdir('data/state'):
                                os.remove(os.path.join('data/state', f))
                                logger.debug(f"Cleared stale state file: {f}")
                        st.session_state.data_manager = DataManager(config)
                        logger.debug(f"Initializing LogProcessor with log_folder: {st.session_state.log_folder}, config: {config}")
                        st.session_state.processor = LogProcessor(st.session_state.log_folder, config, st.session_state.data_manager)
                        st.session_state.app_state = 'RUNNING'
                        st.session_state.last_update_time = time.time()
                        st.session_state.total_lines_processed = 0
                        st.session_state.files_processed = 0
                        st.session_state.total_files = st.session_state.processor.get_total_files()
                        st.session_state.files_remaining = st.session_state.total_files
                        st.session_state.folders_processed = set()
                        st.session_state.current_folder = None
                        st.session_state.average_file_time = 0
                        st.session_state.dashboard_data = {
                            'level_counts_by_class': pd.DataFrame(),
                            'level_counts_by_service': pd.DataFrame(),
                            'timeline_data': pd.DataFrame(),
                            'class_service_counts': pd.DataFrame()
                        }
                        st.session_state.logs_to_display_class = []
                        st.session_state.logs_to_display_service = []
                        st.session_state.total_logs_class = 0
                        st.session_state.total_logs_service = 0
                        st.session_state.log_page = 1
                        st.session_state.selected_index = None
                        st.session_state.selected_level = None
                        st.session_state.pop('search_query_class', None)
                        st.session_state.pop('search_query_service', None)
                        logger.debug(f"Started analysis, total_files: {st.session_state.total_files}, log_folder: {st.session_state.log_folder}, expected gz_files: {gz_files}")
                        st.session_state.notifications.append({'type': 'success', 'message': f"Started fresh analysis for folder: {st.session_state.log_folder}", 'timestamp': time.time()})
                        logger.info(f"Started fresh analysis for folder: {st.session_state.log_folder}")
                except Exception as e:
                    st.session_state.notifications.append({'type': 'error', 'message': f"Error starting analysis: {str(e)}", 'timestamp': time.time()})
                    logger.error(f"Error starting analysis: {str(e)}")
                    st.session_state.app_state = 'IDLE'

        if pause_btn:
            logger.debug(f"Pause Analysis clicked, current_state: {st.session_state.app_state}")
            if st.session_state.app_state != 'RUNNING':
                st.session_state.notifications.append({'type': 'warning', 'message': "No analysis is running to pause.", 'timestamp': time.time()})
                logger.warning("Pause Analysis clicked when not RUNNING")
            elif not st.session_state.processor:
                st.session_state.notifications.append({'type': 'warning', 'message': "No analysis is running to pause.", 'timestamp': time.time()})
                logger.warning("Pause Analysis clicked with no active processor")
            else:
                try:
                    st.session_state.processor.save_state()
                    st.session_state.app_state = 'PAUSED'
                    st.session_state.notifications.append({'type': 'success', 'message': "Analysis paused and state saved", 'timestamp': time.time()})
                    logger.info("Analysis paused")
                    st.session_state.logs_to_display_class = []
                    st.session_state.logs_to_display_service = []
                    st.session_state.total_logs_class = 0
                    st.session_state.total_logs_service = 0
                    st.session_state.log_page = 1
                    st.session_state.selected_index = None
                    st.session_state.selected_level = None
                    st.session_state.pop('search_query_class', None)
                    st.session_state.pop('search_query_service', None)
                except Exception as e:
                    st.session_state.notifications.append({'type': 'error', 'message': f"Error pausing analysis: {str(e)}", 'timestamp': time.time()})
                    logger.error(f"Error pausing analysis: {str(e)}")
                    st.session_state.app_state = 'IDLE'

        if resume_btn:
            logger.debug(f"Resume Analysis clicked, current_state: {st.session_state.app_state}")
            try:
                state_info = get_available_states()
                if not state_info:
                    st.session_state.notifications.append({'type': 'warning', 'message': "No saved states found. Please start a fresh analysis.", 'timestamp': time.time()})
                    logger.warning("No saved states found for resume")
                else:
                    selected_state_file = None
                    if selected_state != "Select a state (default: latest)":
                        selected_idx = state_options.index(selected_state) - 1
                        selected_state_file = state_info[selected_idx][0]
                    else:
                        selected_state_file = state_info[0][0]

                    state = st.session_state.data_manager.load_state(selected_state_file)
                    if not state or not os.path.exists(state.get('log_folder', '')):
                        st.session_state.notifications.append({'type': 'warning', 'message': "Selected state is invalid or log folder does not exist.", 'timestamp': time.time()})
                        logger.warning(f"Invalid state or missing log folder in state: {selected_state_file}")
                    else:
                        logger.debug(f"Resuming with state: {selected_state_file}, log_folder: {state['log_folder']}")
                        st.session_state.processor = LogProcessor(
                            state['log_folder'],
                            config,
                            st.session_state.data_manager,
                            state
                        )
                        st.session_state.log_folder = state['log_folder']
                        st.session_state.last_update_time = time.time()
                        st.session_state.total_lines_processed = state.get('total_lines_processed', 0)
                        st.session_state.files_processed = state.get('files_processed', 0)
                        st.session_state.total_files = st.session_state.processor.get_total_files()
                        st.session_state.files_remaining = st.session_state.processor.get_remaining_files()
                        st.session_state.folders_processed = set(state.get('folders_processed', []))
                        st.session_state.average_file_time = state.get('average_file_time', 0)
                        st.session_state.dashboard_data = {
                            'level_counts_by_class': st.session_state.data_manager.get_level_counts_by_class(),
                            'level_counts_by_service': st.session_state.data_manager.get_level_counts_by_service(),
                            'timeline_data': st.session_state.data_manager.get_timeline_data(),
                            'class_service_counts': st.session_state.data_manager.get_class_service_counts()
                        }
                        st.session_state.app_state = 'RUNNING'
                        st.session_state.logs_to_display_class = []
                        st.session_state.logs_to_display_service = []
                        st.session_state.total_logs_class = 0
                        st.session_state.total_logs_service = 0
                        st.session_state.log_page = 1
                        st.session_state.selected_index = None
                        st.session_state.selected_level = None
                        st.session_state.pop('search_query_class', None)
                        st.session_state.pop('search_query_service', None)
                        logger.debug(f"Resumed analysis, total_files: {st.session_state.total_files}, files_remaining: {st.session_state.files_remaining}, log_folder: {state['log_folder']}")
                        st.session_state.notifications.append({'type': 'success', 'message': f"Resumed analysis from state: {os.path.basename(selected_state_file)}", 'timestamp': time.time()})
                        logger.info(f"Resumed analysis from state: {selected_state_file}")
            except Exception as e:
                st.session_state.notifications.append({'type': 'error', 'message': f"Error resuming analysis: {str(e)}", 'timestamp': time.time()})
                logger.error(f"Error resuming analysis: {str(e)}")
                st.session_state.app_state = 'IDLE'

        if download_btn:
            logger.debug(f"Download Results clicked, current_state: {st.session_state.app_state}")
            try:
                if os.path.exists(st.session_state.data_manager.level_counts_by_class_file):
                    excel_buffer = st.session_state.data_manager.create_excel()
                    st.download_button(
                        label="Download Excel",
                        data=excel_buffer,
                        file_name=f"log_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    st.session_state.notifications.append({'type': 'success', 'message': "Results ready for download.", 'timestamp': time.time()})
                    logger.info("Downloaded analysis results")
                else:
                    st.session_state.notifications.append({'type': 'warning', 'message': "No analysis data available to download.", 'timestamp': time.time()})
                    logger.warning("Download Results clicked with no data")
            except Exception as e:
                st.session_state.notifications.append({'type': 'error', 'message': f"Error creating download: {str(e)}", 'timestamp': time.time()})
                logger.error(f"Error creating download: {str(e)}")

        # Analysis loop
        if st.session_state.app_state == 'RUNNING' and st.session_state.processor:
            try:
                batch_size = 5
                files_processed_in_batch = 0
                processed = True

                while st.session_state.app_state == 'RUNNING' and processed and files_processed_in_batch < batch_size:
                    processed = st.session_state.processor.process_next_file()
                    logger.debug(f"process_next_file returned: {processed}, files_processed: {st.session_state.files_processed}, total_files: {st.session_state.total_files}, current_folder_idx: {st.session_state.processor.current_folder_idx if st.session_state.processor else -1}, current_file: {st.session_state.processor.current_file if st.session_state.processor else 'None'}")
                    if processed:
                        st.session_state.files_processed += 1
                        st.session_state.files_remaining = st.session_state.processor.get_remaining_files()
                        st.session_state.total_lines_processed = st.session_state.processor.total_lines_processed
                        st.session_state.current_folder = st.session_state.processor.folders[st.session_state.processor.current_folder_idx] if st.session_state.processor.current_folder_idx < len(st.session_state.processor.folders) else None
                        st.session_state.current_file = st.session_state.processor.current_file
                        if st.session_state.current_folder:
                            st.session_state.folders_processed.add(st.session_state.current_folder)
                        files_processed_in_batch += 1
                        if st.session_state.processor.last_file_time:
                            total_time = st.session_state.average_file_time * (st.session_state.files_processed - 1) + st.session_state.processor.last_file_time
                            st.session_state.average_file_time = total_time / st.session_state.files_processed
                            logger.debug(f"Processed file: {st.session_state.current_file}, lines: {st.session_state.total_lines_processed}, avg_file_time: {st.session_state.average_file_time:.2f}s")

                if files_processed_in_batch >= batch_size or time.time() - st.session_state.last_update_time >= st.session_state.update_interval or not processed:
                    st.session_state.data_manager.save_data(
                        st.session_state.processor.level_counts_by_class,
                        st.session_state.processor.level_counts_by_service,
                        st.session_state.processor.timeline_data,
                        st.session_state.processor.class_service_counts
                    )
                    st.session_state.dashboard_data = {
                        'level_counts_by_class': st.session_state.data_manager.get_level_counts_by_class(),
                        'level_counts_by_service': st.session_state.data_manager.get_level_counts_by_service(),
                        'timeline_data': st.session_state.data_manager.get_timeline_data(),
                        'class_service_counts': st.session_state.data_manager.get_class_service_counts()
                    }
                    with st.container():
                        render_full_ui()
                    st.session_state.last_update_time = time.time()
                    files_processed_in_batch = 0
                    logger.info("Updated UI with new data")

                if not processed:
                    st.session_state.app_state = 'IDLE'
                    st.session_state.data_manager.save_data(
                        st.session_state.processor.level_counts_by_class,
                        st.session_state.processor.level_counts_by_service,
                        st.session_state.processor.timeline_data,
                        st.session_state.processor.class_service_counts
                    )
                    st.session_state.dashboard_data = {
                        'level_counts_by_class': st.session_state.data_manager.get_level_counts_by_class(),
                        'level_counts_by_service': st.session_state.data_manager.get_level_counts_by_service(),
                        'timeline_data': st.session_state.data_manager.get_timeline_data(),
                        'class_service_counts': st.session_state.data_manager.get_class_service_counts()
                    }
                    with st.container():
                        render_full_ui()
                    if st.session_state.files_processed > 0:
                        st.session_state.notifications.append({'type': 'success', 'message': "Analysis completed", 'timestamp': time.time()})
                        logger.info("Analysis completed")
                    else:
                        st.session_state.notifications.append({'type': 'warning', 'message': "Analysis stopped: No files processed. Check log files or LogProcessor.", 'timestamp': time.time()})
                        logger.warning("Analysis stopped: No files processed")

            except Exception as e:
                st.session_state.notifications.append({'type': 'error', 'message': f"Error during analysis: {str(e)}", 'timestamp': time.time()})
                logger.error(f"Error during analysis: {str(e)}")
                st.session_state.app_state = 'IDLE'

        # Display UI if not running
        if st.session_state.app_state != 'RUNNING':
            with st.container():
                render_full_ui()

    with tab2:
        st.header("CSV Visualization")
        st.markdown("Upload the CSV files generated by the log analysis script to visualize the data.")
        
        # File uploader for multiple CSV files
        uploaded_files = st.file_uploader(
            "Upload CSV Files",
            type=["csv"],
            accept_multiple_files=True,
            key="csv_uploader"
        )

        if uploaded_files:
            try:
                # Clear previous data
                st.session_state.uploaded_csv_data = {}
                
                # Expected analysis names
                expected_analyses = [
                    'class_level_counts',
                    'level_summary',
                    'class_summary',
                    'pod_summary',
                    'container_summary',
                    'host_summary',
                    'class_level_pod',
                    'hourly_level_counts',
                    'thread_summary',
                    'error_analysis',
                    'time_range'
                ]
                
                # Process each uploaded file
                for uploaded_file in uploaded_files:
                    file_name = uploaded_file.name
                    # Extract analysis name by removing timestamp suffix (e.g., time_range_20241010_120000.csv -> time_range)
                    match = re.match(r'^(.+?)_\d{8}_\d{6}\.csv$', file_name)
                    if match:
                        analysis_name = match.group(1)
                        if analysis_name in expected_analyses:
                            df = pd.read_csv(uploaded_file)
                            st.session_state.uploaded_csv_data[analysis_name] = df
                            logger.debug(f"Loaded CSV: {file_name}, analysis: {analysis_name}, shape: {df.shape}, columns: {df.columns.tolist()}")
                        else:
                            logger.warning(f"Ignored CSV with unrecognized analysis name: {file_name}")
                    else:
                        logger.warning(f"Ignored CSV with invalid filename format: {file_name}")

                if st.session_state.uploaded_csv_data:
                    loaded_analyses = list(st.session_state.uploaded_csv_data.keys())
                    st.session_state.notifications.append({
                        'type': 'success',
                        'message': f"Successfully loaded {len(loaded_analyses)} CSV files: {', '.join(loaded_analyses)}.",
                        'timestamp': time.time()
                    })
                    # Display the CSV visualization dashboard
                    st.session_state.visualizer.display_csv_dashboard(st.session_state.uploaded_csv_data)
                else:
                    st.session_state.notifications.append({
                        'type': 'warning',
                        'message': "No valid CSV files uploaded. Ensure filenames match expected patterns (e.g., time_range_YYYYMMDD_HHMMSS.csv).",
                        'timestamp': time.time()
                    })
            except Exception as e:
                st.session_state.notifications.append({
                    'type': 'error',
                    'message': f"Error processing CSV files: {str(e)}",
                    'timestamp': time.time()
                })
                logger.error(f"Error processing CSV files: {str(e)}")

if __name__ == "__main__":
    main()