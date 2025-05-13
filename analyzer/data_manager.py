import sqlite3
import pandas as pd
import logging
import os
import xlsxwriter
import streamlit as st
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='log_analyzer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_db():
    """Initialize SQLite database with jobs, logs, metadata, and summary tables."""
    try:
        os.makedirs('data', exist_ok=True)
        conn = sqlite3.connect('data/logs.db', timeout=30)
        cursor = conn.cursor()
        
        # Optimize SQLite settings
        cursor.execute('PRAGMA synchronous = OFF')
        cursor.execute('PRAGMA journal_mode = WAL')
        cursor.execute('PRAGMA cache_size = -20000')  # 20MB cache
        
        # Jobs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                folder_path TEXT,
                status TEXT,
                files_processed INTEGER,
                total_files INTEGER,
                start_time TEXT,
                last_updated TEXT,
                current_file TEXT
            )
        ''')
        
        # Logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                timestamp TEXT,
                level TEXT,
                class TEXT,
                service TEXT,
                log_message TEXT,
                folder TEXT,
                file_name TEXT,
                line_idx INTEGER,
                FOREIGN KEY (job_id) REFERENCES jobs (job_id)
            )
        ''')
        
        # Job metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_metadata (
                job_id TEXT,
                type TEXT,
                value TEXT,
                UNIQUE(job_id, type, value)
            )
        ''')
        
        # Summary tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS class_level_counts (
                job_id TEXT,
                class TEXT,
                level TEXT,
                count INTEGER,
                PRIMARY KEY (job_id, class, level)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_level_counts (
                job_id TEXT,
                service TEXT,
                level TEXT,
                count INTEGER,
                PRIMARY KEY (job_id, service, level)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS timeline_counts (
                job_id TEXT,
                hour TEXT,
                level TEXT,
                count INTEGER,
                PRIMARY KEY (job_id, hour, level)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS class_service_counts (
                job_id TEXT,
                class TEXT,
                service TEXT,
                count INTEGER,
                PRIMARY KEY (job_id, class, service)
            )
        ''')
        
        # Optimized indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_job_id ON logs (job_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_class ON logs (class)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_service ON logs (service)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_level ON logs (level)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_job_id_class_level ON logs (job_id, class, level)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_job_id_service_level ON logs (job_id, service, level)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_job_id_class_timestamp_level ON logs (job_id, class, timestamp, level)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_job_id_service_timestamp_level ON logs (job_id, service, timestamp, level)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_job_metadata_job_id_type ON job_metadata (job_id, type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_class_level_counts_job_id ON class_level_counts (job_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_service_level_counts_job_id ON service_level_counts (job_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timeline_counts_job_id ON timeline_counts (job_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_class_service_counts_job_id ON class_service_counts (job_id)')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized with optimized tables and indexes")
    except sqlite3.OperationalError as e:
        logger.error(f"Database initialization error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

@st.cache_data(hash_funcs={str: lambda x: x})
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

@st.cache_data(hash_funcs={str: lambda x: x})
def get_logs_by_class_and_level(job_id: str, class_name: str, level: str, page: int, logs_per_page: int, search_query: str = None, use_regex: bool = False):
    """Retrieve logs by class and level from SQLite, cached."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        cursor = conn.cursor()
        offset = (page - 1) * logs_per_page
        
        # Log query parameters
        logger.debug(f"get_logs_by_class_and_level: job_id={job_id}, class={class_name}, level={level}, page={page}, logs_per_page={logs_per_page}, search_query={search_query}, use_regex={use_regex}")
        
        # Base query
        if level == "ALL":
            query = """
                SELECT timestamp, log_message, level, class
                FROM logs
                WHERE job_id = ? AND class = ?
            """
            params = [job_id, class_name]
        else:
            query = """
                SELECT timestamp, log_message, level, class
                FROM logs
                WHERE job_id = ? AND class = ? AND level = ?
            """
            params = [job_id, class_name, level]
        
        # Add search query if provided
        if search_query and search_query.strip():
            if use_regex:
                query += " AND log_message REGEXP ?"
                params.append(search_query)
            else:
                query += " AND log_message LIKE ?"
                params.append(f'%{search_query}%')
        
        # Add sorting and pagination
        query += " ORDER BY timestamp LIMIT ? OFFSET ?"
        params.extend([logs_per_page, offset])
        
        # Log the exact query
        logger.debug(f"Executing SQL: {query} with params: {params}")
        
        # Execute data query
        cursor.execute(query, params)
        logs = [
            {"timestamp": row[0], "log_message": row[1], "level": row[2], "class": row[3]}
            for row in cursor.fetchall()
        ]
        
        # Count query
        if level == "ALL":
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND class = ?
            """
            count_params = [job_id, class_name]
        else:
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND class = ? AND level = ?
            """
            count_params = [job_id, class_name, level]
        
        if search_query and search_query.strip():
            if use_regex:
                count_query += " AND log_message REGEXP ?"
                count_params.append(search_query)
            else:
                count_query += " AND log_message LIKE ?"
                count_params.append(f'%{search_query}%')
        
        # Execute count query
        cursor.execute(count_query, count_params)
        total_logs = cursor.fetchone()[0]
        
        conn.close()
        logger.debug(f"Fetched {len(logs)} logs, total_logs={total_logs}, page={page}")
        return logs, total_logs
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
'           timestamp': time.time()
        })
        raise

@st.cache_data(hash_funcs={str: lambda x: x})
def get_logs_by_service_and_level(job_id: str, service_name: str, level: str, page: int, logs_per_page: int, search_query: str = None, use_regex: bool = False):
    """Retrieve logs by service and level from SQLite, cached."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        cursor = conn.cursor()
        offset = (page - 1) * logs_per_page
        
        # Log query parameters
        logger.debug(f"get_logs_by_service_and_level: job_id={job_id}, service={service_name}, level={level}, page={page}, logs_per_page={logs_per_page}, search_query={search_query}, use_regex={use_regex}")
        
        # Base query
        if level == "ALL":
            query = """
                SELECT timestamp, log_message, level, service
                FROM logs
                WHERE job_id = ? AND service = ?
            """
            params = [job_id, service_name]
        else:
            query = """
                SELECT timestamp, log_message, level, service
                FROM logs
                WHERE job_id = ? AND service = ? AND level = ?
            """
            params = [job_id, service_name, level]
        
        # Add search query if provided
        if search_query and search_query.strip():
            if use_regex:
                query += " AND log_message REGEXP ?"
                params.append(search_query)
            else:
                query += " AND log_message LIKE ?"
                params.append(f'%{search_query}%')
        
        # Add sorting and pagination
        query += " ORDER BY timestamp LIMIT ? OFFSET ?"
        params.extend([logs_per_page, offset])
        
        # Log the exact query
        logger.debug(f"Executing SQL: {query} with params: {params}")
        
        # Execute data query
        cursor.execute(query, params)
        logs = [
            {"timestamp": row[0], "log_message": row[1], "level": row[2], "service": row[3]}
            for row in cursor.fetchall()
        ]
        
        # Count query
        if level == "ALL":
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND service = ?
            """
            count_params = [job_id, service_name]
        else:
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND service = ? AND level = ?
            """
            count_params = [job_id, service_name, level]
        
        if search_query and search_query.strip():
            if use_regex:
                count_query += " AND log_message REGEXP ?"
                count_params.append(search_query)
            else:
                count_query += " AND log_message LIKE ?"
                count_params.append(f'%{search_query}%')
        
        # Execute count query
        cursor.execute(count_query, count_params)
        total_logs = cursor.fetchone()[0]
        
        conn.close()
        logger.debug(f"Fetched {len(logs)} logs, total_logs={total_logs}, page={page}")
        return logs, total_logs
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

@st.cache_data(hash_funcs={str: lambda x: x})
def _fetch_analysis_data(job_id: str, query_type: str) -> pd.DataFrame:
    """Fetch analysis data for a specific query type from summary tables."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        
        if query_type == 'class':
            df = pd.read_sql_query("""
                SELECT class, level, count
                FROM class_level_counts
                WHERE job_id = ?
            """, conn, params=[job_id])
        
        elif query_type == 'service':
            df = pd.read_sql_query("""
                SELECT service, level, count
                FROM service_level_counts
                WHERE job_id = ?
            """, conn, params=[job_id])
        
        elif query_type == 'timeline':
            df = pd.read_sql_query("""
                SELECT hour, level, count
                FROM timeline_counts
                WHERE job_id = ?
                ORDER BY hour
            """, conn, params=[job_id])
            # Convert hour to datetime for consistent plotting
            if not df.empty:
                df['hour'] = pd.to_datetime(df['hour'], format='%Y-%m-%d %H:00:00', errors='coerce')
                df = df.dropna(subset=['hour'])  # Drop rows with invalid datetime
        
        elif query_type == 'class_service':
            df = pd.read_sql_query("""
                SELECT class, service, count
                FROM class_service_counts
                WHERE job_id = ?
            """, conn, params=[job_id])
        
        else:
            raise ValueError(f"Invalid query_type: {query_type}")
        
        conn.close()
        
        if df.empty:
            if query_type == 'class':
                df = pd.DataFrame(columns=['class', 'level', 'count'])
            elif query_type == 'service':
                df = pd.DataFrame(columns=['service', 'level', 'count'])
            elif query_type == 'timeline':
                df = pd.DataFrame(columns=['hour', 'level', 'count'])
            elif query_type == 'class_service':
                df = pd.DataFrame(columns=['class', 'service', 'count'])
        
        logger.info(f"Retrieved {query_type} data for job_id: {job_id}, rows: {len(df)}")
        return df
    
    except sqlite3.OperationalError as e:
        logger.error(f"Database error retrieving {query_type} data for job_id {job_id}: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error retrieving {query_type} data for job_id {job_id}: {str(e)}")
        return pd.DataFrame()

def get_analysis_data(job_id: str, query_type: str) -> pd.DataFrame:
    """Retrieve analysis data for a job."""
    return _fetch_analysis_data(job_id, query_type)

def export_to_excel(job_id: str) -> str:
    """Export analysis data to Excel file."""
    try:
        level_counts_by_class = get_analysis_data(job_id=job_id, query_type='class')
        level_counts_by_service = get_analysis_data(job_id=job_id, query_type='service')
        timeline_data = get_analysis_data(job_id=job_id, query_type='timeline')
        
        # Pivot class data
        if not level_counts_by_class.empty:
            class_pivot = level_counts_by_class.pivot(index='class', columns='level', values='count').fillna(0)
            class_pivot = class_pivot.reset_index()
        else:
            class_pivot = pd.DataFrame(columns=['class'])
        
        # Pivot service data
        if not level_counts_by_service.empty:
            service_pivot = level_counts_by_service.pivot(index='service', columns='level', values='count').fillna(0)
            service_pivot = service_pivot.reset_index()
        else:
            service_pivot = pd.DataFrame(columns=['service'])
        
        # Calculate total counts for class and service
        class_totals = level_counts_by_class.groupby('class')['count'].sum().reset_index()
        service_totals = level_counts_by_service.groupby('service')['count'].sum().reset_index()
        
        # Create folder with job_id and timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join('data', 'exports', f'{job_id}_{timestamp}')
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'analysis_results.xlsx')
        
        with xlsxwriter.Workbook(output_file) as workbook:
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#12133f',
                'font_color': '#FFFFFF',
                'border': 1
            })
            cell_format = workbook.add_format({'border': 1})
            
            # Class Level Counts
            worksheet1 = workbook.add_worksheet('Class Level Counts')
            headers1 = ['Class'] + list(class_pivot.columns[1:])
            for col, header in enumerate(headers1):
                worksheet1.write(0, col, header, header_format)
            for row, data in enumerate(class_pivot.to_dict('records'), 1):
                worksheet1.write(row, 0, data['class'], cell_format)
                for col, level in enumerate(class_pivot.columns[1:], 1):
                    worksheet1.write(row, col, data.get(level, 0), cell_format)
            
            # Service Level Counts
            worksheet2 = workbook.add_worksheet('Service Level Counts')
            headers2 = ['Service'] + list(service_pivot.columns[1:])
            for col, header in enumerate(headers2):
                worksheet2.write(0, col, header, header_format)
            for row, data in enumerate(service_pivot.to_dict('records'), 1):
                worksheet2.write(row, 0, data['service'], cell_format)
                for col, level in enumerate(service_pivot.columns[1:], 1):
                    worksheet2.write(row, col, data.get(level, 0), cell_format)
            
            # Timeline Data
            worksheet3 = workbook.add_worksheet('Timeline Data')
            headers3 = ['Hour', 'Level', 'Count']
            for col, header in enumerate(headers3):
                worksheet3.write(0, col, header, header_format)
            for row, data in enumerate(timeline_data.to_dict('records'), 1):
                worksheet3.write(row, 0, data['hour'], cell_format)
                worksheet3.write(row, 1, data['level'], cell_format)
                worksheet3.write(row, 2, data['count'], cell_format)
            
            # Class Totals
            worksheet4 = workbook.add_worksheet('Class Totals')
            headers4 = ['Class', 'Count']
            for col, header in enumerate(headers4):
                worksheet4.write(0, col, header, header_format)
            for row, data in enumerate(class_totals.to_dict('records'), 1):
                worksheet4.write(row, 0, data['class'], cell_format)
                worksheet4.write(row, 1, data['count'], cell_format)
            
            # Service Totals
            worksheet5 = workbook.add_worksheet('Service Totals')
            headers5 = ['Service', 'Count']
            for col, header in enumerate(headers5):
                worksheet5.write(0, col, header, header_format)
            for row, data in enumerate(service_totals.to_dict('records'), 1):
                worksheet5.write(row, 0, data['service'], cell_format)
                worksheet5.write(row, 1, data['count'], cell_format)
        
        logger.info(f"Exported analysis data to {output_file} for job_id: {job_id}")
        return output_file
    
    except Exception as e:
        logger.error(f"Error exporting to Excel for job_id {job_id}: {str(e)}")
        raise