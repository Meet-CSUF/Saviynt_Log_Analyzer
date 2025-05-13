import asyncio
import gzip
import json
import os
import sqlite3
import logging
import pandas as pd
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Optional
from analyzer.data_manager import init_db
from yaml import safe_load
from retrying import retry

# Configure logging
logging.basicConfig(
    filename='log_analyzer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Saviynt Log Analyzer Backend")

# Global job state
job_states: Dict[str, Dict] = {}
db_initialized = False

class StartJobRequest(BaseModel):
    folder_path: str

class JobResponse(BaseModel):
    job_id: str
    folder_path: str
    status: str
    files_processed: int
    total_files: int
    start_time: str
    last_updated: str

def load_config():
    """Load configuration from YAML file."""
    try:
        with open('config/config.yaml', 'r') as f:
            return safe_load(f)
    except FileNotFoundError:
        logger.error("Config file config/config.yaml not found")
        raise HTTPException(status_code=500, detail="Configuration file not found")
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error loading config: {str(e)}")

config = load_config()

def update_summary_tables(conn: sqlite3.Connection, job_id: str, batch: list):
    """Update summary tables with batched log entries."""
    try:
        cursor = conn.cursor()
        class_level_batch = {}
        service_level_batch = {}
        timeline_batch = {}
        class_service_batch = {}
        invalid_timestamp_count = 0
        
        for log_entry in batch:
            level = log_entry.get('level', 'UNKNOWN')
            class_name = log_entry.get('class', 'Unknown')
            service = log_entry.get('service', class_name)
            timestamp = log_entry.get('logtime', '')
            
            if class_name and level:
                key = (job_id, class_name, level)
                class_level_batch[key] = class_level_batch.get(key, 0) + 1
            
            if service and level:
                key = (job_id, service, level)
                service_level_batch[key] = service_level_batch.get(key, 0) + 1
            
            if timestamp and level:
                try:
                    # Try parsing with milliseconds
                    try:
                        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S,%f')
                    except ValueError:
                        # Try parsing without milliseconds
                        try:
                            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            # Try parsing Apache-like format with timezone
                            dt = datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S %z')
                    hour = dt.strftime('%Y-%m-%d %H:00:00')
                    key = (job_id, hour, level)
                    timeline_batch[key] = timeline_batch.get(key, 0) + 1
                except ValueError:
                    invalid_timestamp_count += 1
            
            if class_name and service:
                key = (job_id, class_name, service)
                class_service_batch[key] = class_service_batch.get(key, 0) + 1
        
        for (job_id, class_name, level), count in class_level_batch.items():
            cursor.execute('''
                INSERT INTO class_level_counts (job_id, class, level, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id, class, level) DO UPDATE SET count = count + ?
            ''', (job_id, class_name, level, count, count))
        
        for (job_id, service, level), count in service_level_batch.items():
            cursor.execute('''
                INSERT INTO service_level_counts (job_id, service, level, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id, service, level) DO UPDATE SET count = count + ?
            ''', (job_id, service, level, count, count))
        
        for (job_id, hour, level), count in timeline_batch.items():
            cursor.execute('''
                INSERT INTO timeline_counts (job_id, hour, level, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id, hour, level) DO UPDATE SET count = count + ?
            ''', (job_id, hour, level, count, count))
        
        for (job_id, class_name, service), count in class_service_batch.items():
            cursor.execute('''
                INSERT INTO class_service_counts (job_id, class, service, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id, class, service) DO UPDATE SET count = count + ?
            ''', (job_id, class_name, service, count, count))
        
        conn.commit()
        if invalid_timestamp_count > 0:
            logger.debug(f"Skipped {invalid_timestamp_count} log entries with invalid timestamps in job_id: {job_id}")
    except sqlite3.OperationalError as e:
        logger.error(f"Error updating summary tables for job_id {job_id}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error updating summary tables for job_id {job_id}: {str(e)}")

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
async def process_log_file(file_path: str, job_id: str, conn: sqlite3.Connection):
    """Process a single .gz log file and insert logs into SQLite with retries."""
    try:
        valid_levels = set(config['app']['log_levels'])
        batch_size = 500
        log_batch = []
        log_entries = []
        classes = set()
        services = set()
        missing_class_count = 0
        invalid_timestamp_count = 0
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line_idx, line in enumerate(f):
                try:
                    log_entry = json.loads(line.strip())
                    timestamp = log_entry.get('logtime', '')
                    level = log_entry.get('level', 'UNKNOWN')
                    if level not in valid_levels:
                        level = 'UNKNOWN'
                    class_field = log_entry.get('class', None)
                    log_message = log_entry.get('log', '')
                    folder = os.path.dirname(file_path)
                    file_name = os.path.basename(file_path)
                    
                    # Extract class and service
                    if class_field and '.' in class_field:
                        service, class_name = class_field.split('.', 1)
                    else:
                        class_name = 'Unknown'
                        service = 'Unknown'
                        missing_class_count += 1
                    
                    # Validate timestamp
                    if timestamp:
                        try:
                            try:
                                datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S,%f')
                            except ValueError:
                                try:
                                    datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S %z')
                        except ValueError:
                            invalid_timestamp_count += 1
                    
                    log_batch.append((job_id, timestamp, level, class_name, service, log_message, folder, file_name, line_idx))
                    log_entries.append({
                        'logtime': timestamp,
                        'level': level,
                        'class': class_name,
                        'service': service,
                        'log': log_message
                    })
                    classes.add(class_name)
                    services.add(service)
                    
                    if len(log_batch) >= batch_size:
                        conn.executemany('''
                            INSERT INTO logs (job_id, timestamp, level, class, service, log_message, folder, file_name, line_idx)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', log_batch)
                        update_summary_tables(conn, job_id, log_entries)
                        
                        for class_name in classes:
                            conn.execute('''
                                INSERT OR IGNORE INTO job_metadata (job_id, type, value)
                                VALUES (?, ?, ?)
                            ''', (job_id, 'class', class_name))
                        for service in services:
                            conn.execute('''
                                INSERT OR IGNORE INTO job_metadata (job_id, type, value)
                                VALUES (?, ?, ?)
                            ''', (job_id, 'service', service))
                        
                        conn.commit()
                        log_batch = []
                        log_entries = []
                        classes.clear()
                        services.clear()
                        await asyncio.sleep(0)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in {file_path} at line {line_idx}")
                except Exception as e:
                    logger.error(f"Error processing line {line_idx} in {file_path}: {str(e)}")
        
        if log_batch:
            conn.executemany('''
                INSERT INTO logs (job_id, timestamp, level, class, service, log_message, folder, file_name, line_idx)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', log_batch)
            update_summary_tables(conn, job_id, log_entries)
            
            for class_name in classes:
                conn.execute('''
                    INSERT OR IGNORE INTO job_metadata (job_id, type, value)
                    VALUES (?, ?, ?)
                ''', (job_id, 'class', class_name))
            for service in services:
                conn.execute('''
                    INSERT OR IGNORE INTO job_metadata (job_id, type, value)
                    VALUES (?, ?, ?)
                ''', (job_id, 'service', service))
            
            conn.commit()
        
        # Log the processed file in the database
        conn.execute('''
            INSERT INTO job_metadata (job_id, type, value)
            VALUES (?, ?, ?)
        ''', (job_id, 'processed_file', file_path))
        conn.commit()
        
        logger.info(f"Processed log file: {file_path} for job_id: {job_id}, "
                   f"missing or invalid class formats: {missing_class_count}, "
                   f"invalid timestamps: {invalid_timestamp_count}")
    except Exception as e:
        logger.error(f"Error processing log file {file_path}: {str(e)}")
        raise

async def process_job(job_id: str, folder_path: str):
    """Process all log files in the specified folder, resuming from last processed file."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=60)
        conn.execute('PRAGMA journal_mode=WAL')
        
        # Validate folder path
        if not os.path.isdir(folder_path):
            logger.error(f"Invalid folder path: {folder_path}")
            conn.execute('''
                UPDATE jobs SET status = ?, last_updated = ?, total_files = ?, files_processed = ?
                WHERE job_id = ?
            ''', ('ERROR', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 0, 0, job_id))
            conn.commit()
            job_states[job_id]['status'] = 'ERROR'
            job_states[job_id]['files_processed'] = 0
            job_states[job_id]['total_files'] = 0
            conn.close()
            raise HTTPException(status_code=400, detail=f"Invalid folder path: {folder_path}")
        
        # Recursively find .gz files
        log_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.gz'):
                    full_path = os.path.join(root, file)
                    log_files.append(full_path)
                    logger.debug(f"Found log file: {full_path}")
        
        total_files = len(log_files)
        if total_files == 0:
            logger.warning(f"No .gz files found in folder: {folder_path}")
            conn.execute('''
                UPDATE jobs SET status = ?, last_updated = ?, total_files = ?, files_processed = ?
                WHERE job_id = ?
            ''', ('COMPLETED', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 0, 0, job_id))
            conn.commit()
            job_states[job_id]['status'] = 'COMPLETED'
            job_states[job_id]['files_processed'] = 0
            job_states[job_id]['total_files'] = 0
            conn.close()
            return
        
        # Get already processed files
        cursor = conn.cursor()
        cursor.execute('''
            SELECT value FROM job_metadata WHERE job_id = ? AND type = 'processed_file'
        ''', (job_id,))
        processed_files = set(row[0] for row in cursor.fetchall())
        files_processed = len(processed_files)
        logger.info(f"Job {job_id} resuming with {files_processed}/{total_files} files already processed")
        
        # Update job metadata
        conn.execute('''
            UPDATE jobs SET status = ?, total_files = ?, files_processed = ?, last_updated = ?
            WHERE job_id = ?
        ''', ('RUNNING', total_files, files_processed, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), job_id))
        conn.commit()
        job_states[job_id]['total_files'] = total_files
        job_states[job_id]['files_processed'] = files_processed
        
        # Process remaining files
        for idx, file_path in enumerate(log_files):
            if file_path in processed_files:
                logger.debug(f"Skipping already processed file: {file_path}")
                continue
            
            if job_states[job_id]['status'] == 'PAUSED':
                logger.info(f"Job {job_id} paused at file {file_path}")
                conn.execute('''
                    UPDATE jobs SET status = ?, last_updated = ?, files_processed = ?
                    WHERE job_id = ?
                ''', ('PAUSED', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), job_states[job_id]['files_processed'], job_id))
                conn.commit()
                conn.close()
                return
            
            logger.info(f"Processing file {file_path} for job {job_id}")
            await process_log_file(file_path, job_id, conn)
            job_states[job_id]['files_processed'] += 1
            job_states[job_id]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            conn.execute('''
                UPDATE jobs SET files_processed = ?, current_file = ?, last_updated = ?
                WHERE job_id = ?
            ''', (job_states[job_id]['files_processed'], os.path.basename(file_path), job_states[job_id]['last_updated'], job_id))
            conn.commit()
        
        # Mark job as completed
        job_states[job_id]['status'] = 'COMPLETED'
        job_states[job_id]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            UPDATE jobs SET status = ?, last_updated = ?, files_processed = ?, total_files = ?
            WHERE job_id = ?
        ''', ('COMPLETED', job_states[job_id]['last_updated'], job_states[job_id]['files_processed'], total_files, job_id))
        conn.commit()
        
        conn.close()
        logger.info(f"Completed job: {job_id} with {job_states[job_id]['files_processed']}/{total_files} files processed")
    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
        job_states[job_id]['status'] = 'ERROR'
        job_states[job_id]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            UPDATE jobs SET status = ?, last_updated = ?, files_processed = ?, total_files = ?
            WHERE job_id = ?
        ''', ('ERROR', job_states[job_id]['last_updated'], job_states[job_id]['files_processed'], job_states[job_id]['total_files'], job_id))
        conn.commit()
        conn.close()
        raise

@app.on_event("startup")
async def startup_event():
    """Initialize database and load job states on startup."""
    global db_initialized, job_states
    if not db_initialized:
        try:
            init_db()
            db_initialized = True
            logger.info("Database initialized on backend startup")
            
            # Load job states from jobs table
            try:
                conn = sqlite3.connect('data/logs.db', timeout=60)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT job_id, folder_path, status, files_processed, total_files, start_time, last_updated
                    FROM jobs
                ''')
                jobs = cursor.fetchall()
                conn.close()
                
                for job in jobs:
                    job_id, folder_path, status, files_processed, total_files, start_time, last_updated = job
                    job_states[job_id] = {
                        'job_id': job_id,
                        'folder_path': folder_path,
                        'status': status,
                        'files_processed': files_processed,
                        'total_files': total_files,
                        'current_file': '',
                        'start_time': start_time,
                        'last_updated': last_updated
                    }
                logger.info(f"Loaded {len(jobs)} job states from database")
            except sqlite3.OperationalError as e:
                logger.error(f"Error loading job states: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error loading job states: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to initialize database")

@app.get("/health")
async def health_check():
    """Check backend health."""
    return {"status": "healthy"}

@app.post("/jobs/start", response_model=JobResponse)
async def start_job(request: StartJobRequest):
    """Start a new log analysis job."""
    global job_states
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    job_id = request.folder_path.split("/")[-1] + "_" + start_time
    
    job_states[job_id] = {
        'job_id': job_id,
        'folder_path': request.folder_path,
        'status': 'RUNNING',
        'files_processed': 0,
        'total_files': 0,
        'current_file': '',
        'start_time': start_time,
        'last_updated': start_time
    }
    
    try:
        conn = sqlite3.connect('data/logs.db', timeout=60)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('''
            INSERT INTO jobs (job_id, folder_path, status, files_processed, total_files, start_time, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            job_id,
            request.folder_path,
            'RUNNING',
            0,
            0,
            start_time,
            start_time
        ))
        conn.commit()
        conn.close()
        
        asyncio.create_task(process_job(job_id, request.folder_path))
        logger.info(f"Started job: {job_id} for folder: {request.folder_path}")
        return job_states[job_id]
    except Exception as e:
        logger.error(f"Error starting job {job_id}: {str(e)}")
        job_states[job_id]['status'] = 'ERROR'
        job_states[job_id]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        raise HTTPException(status_code=500, detail=f"Error starting job: {str(e)}")

@app.get("/jobs/{job_id}/status", response_model=JobResponse)
async def get_job_status(job_id: str):
    """Get status of a specific job."""
    if job_id not in job_states:
        logger.error(f"Job not found: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")
    logger.debug(f"Retrieved status for job: {job_id}")
    return job_states[job_id]

@app.get("/jobs/{job_id}/processed_files")
async def get_processed_files(job_id: str):
    """Get list of processed files for a specific job."""
    if job_id not in job_states:
        logger.error(f"Job not found: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")
    try:
        conn = sqlite3.connect('data/logs.db', timeout=60)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT value FROM job_metadata WHERE job_id = ? AND type = 'processed_file'
        ''', (job_id,))
        processed_files = [row[0] for row in cursor.fetchall()]
        conn.close()
        logger.debug(f"Retrieved {len(processed_files)} processed files for job: {job_id}")
        return {"job_id": job_id, "processed_files": processed_files}
    except Exception as e:
        logger.error(f"Error retrieving processed files for job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving processed files: {str(e)}")

@app.post("/jobs/{job_id}/pause")
async def pause_job(job_id: str):
    """Pause a running job."""
    if job_id not in job_states:
        logger.error(f"Job not found: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")
    if job_states[job_id]['status'] != 'RUNNING':
        logger.warning(f"Cannot pause job {job_id}: Current status {job_states[job_id]['status']}")
        raise HTTPException(status_code=400, detail=f"Cannot pause job in {job_states[job_id]['status']} status")
    
    try:
        job_states[job_id]['status'] = 'PAUSED'
        job_states[job_id]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        conn = sqlite3.connect('data/logs.db', timeout=60)
        conn.execute('''
            UPDATE jobs
            SET status = ?, last_updated = ?
            WHERE job_id = ?
        ''', (job_states[job_id]['status'], job_states[job_id]['last_updated'], job_id))
        conn.commit()
        conn.close()
        
        logger.info(f"Paused job: {job_id}")
        return {"status": "Job paused"}
    except Exception as e:
        logger.error(f"Error pausing job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error pausing job: {str(e)}")

@app.post("/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    """Resume a paused job."""
    if job_id not in job_states:
        logger.error(f"Job not found: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")
    if job_states[job_id]['status'] != 'PAUSED':
        logger.warning(f"Cannot resume job {job_id}: Current status {job_states[job_id]['status']}")
        raise HTTPException(status_code=400, detail=f"Cannot resume job in {job_states[job_id]['status']} status")
    
    try:
        job_states[job_id]['status'] = 'RUNNING'
        job_states[job_id]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        conn = sqlite3.connect('data/logs.db', timeout=60)
        conn.execute('''
            UPDATE jobs
            SET status = ?, last_updated = ?
            WHERE job_id = ?
        ''', (job_states[job_id]['status'], job_states[job_id]['last_updated'], job_id))
        conn.commit()
        conn.close()
        
        asyncio.create_task(process_job(job_id, job_states[job_id]['folder_path']))
        logger.info(f"Resumed job: {job_id} from {job_states[job_id]['files_processed']} files processed")
        return {"status": "Job resumed"}
    except Exception as e:
        logger.error(f"Error resuming job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error resuming job: {str(e)}")

@app.post("/jobs/{job_id}/delete")
async def delete_job(job_id: str):
    """Delete a job and all its associated data from the database."""
    if job_id not in job_states:
        logger.error(f"Job not found: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")
    
    try:
        conn = sqlite3.connect('data/logs.db', timeout=60)
        conn.execute('PRAGMA journal_mode=WAL')
        cursor = conn.cursor()
        
        # Begin transaction
        cursor.execute('BEGIN TRANSACTION')
        
        # Delete from all relevant tables
        cursor.execute('DELETE FROM jobs WHERE job_id = ?', (job_id,))
        cursor.execute('DELETE FROM logs WHERE job_id = ?', (job_id,))
        cursor.execute('DELETE FROM job_metadata WHERE job_id = ?', (job_id,))
        cursor.execute('DELETE FROM class_level_counts WHERE job_id = ?', (job_id,))
        cursor.execute('DELETE FROM service_level_counts WHERE job_id = ?', (job_id,))
        cursor.execute('DELETE FROM timeline_counts WHERE job_id = ?', (job_id,))
        cursor.execute('DELETE FROM class_service_counts WHERE job_id = ?', (job_id,))
        
        # Commit transaction
        conn.commit()
        
        # Remove from job_states
        del job_states[job_id]
        
        conn.close()
        logger.info(f"Deleted job {job_id} and all associated data")
        return {"status": "Job deleted successfully"}
    except Exception as e:
        # Rollback transaction on error
        conn.rollback()
        conn.close()
        logger.error(f"Error deleting job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting job: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)