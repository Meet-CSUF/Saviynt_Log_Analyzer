import os
import gzip
import json
import pandas as pd
import logging
from datetime import datetime
from collections import defaultdict
import re
import time

logger = logging.getLogger(__name__)

class LogProcessor:
    def __init__(self, log_folder, config, data_manager, state=None):
        """Initialize log processor."""
        self.log_folder = log_folder
        self.config = config
        self.data_manager = data_manager
        self.level_counts_by_class = pd.DataFrame(columns=['class', 'level', 'count'])
        self.level_counts_by_service = pd.DataFrame(columns=['service', 'level', 'count'])
        self.timeline_data = pd.DataFrame(columns=['timestamp', 'level', 'count'])
        self.class_service_counts = pd.DataFrame(columns=['class', 'service', 'count'])
        self.total_lines_processed = state.get('total_lines_processed', 0) if state else 0
        self.files_processed = state.get('files_processed', 0) if state else 0
        self.folders_processed = set(state.get('folders_processed', [])) if state else set()
        self.last_file_time = 0  # Time taken to process the last file
        # Initialize log_index
        self.log_index = defaultdict(list)
        if state and 'log_index_flat' in state:
            # Reconstruct log_index from flat list
            for entry in state['log_index_flat']:
                class_or_service, level, folder, file_name, line_idx = entry
                self.log_index[(class_or_service, level)].append((folder, file_name, line_idx))
        
        # Load state if provided
        if state:
            self.current_folder_idx = state.get('current_folder_idx', 0)
            self.current_file = state.get('current_file')
            self.current_line = state.get('current_line', 0)
            self.average_file_time = state.get('average_file_time', 0)
            # Load existing DataFrames from DataManager
            self.level_counts_by_class = self.data_manager.load_raw_level_counts_by_class()
            self.level_counts_by_service = self.data_manager.load_raw_level_counts_by_service()
            self.timeline_data = self.data_manager.load_raw_timeline_data()
            self.class_service_counts = self.data_manager.load_raw_class_service_counts()
            logger.info(f"Loaded state: current_file={self.current_file}, current_line={self.current_line}, "
                       f"files_processed={self.files_processed}, average_file_time={self.average_file_time}")
        else:
            self.current_folder_idx = 0
            self.current_file = None
            self.current_line = 0
            self.average_file_time = 0

        # Get sorted list of folders
        self.folders = sorted([
            f for f in os.listdir(log_folder)
            if os.path.isdir(os.path.join(log_folder, f)) and f.startswith('20')
        ])
        # Cache file lists for each folder
        self.file_cache = {}
        for folder in self.folders:
            folder_path = os.path.join(self.log_folder, folder)
            self.file_cache[folder] = sorted([f for f in os.listdir(folder_path) if f.endswith('.gz')])

    def get_total_files(self):
        """Get total number of .gz files in all folders."""
        try:
            total = sum(len(files) for files in self.file_cache.values())
            logger.debug(f"Total files to process: {total}")
            return total
        except Exception as e:
            logger.error(f"Error counting total files: {str(e)}")
            raise

    def get_remaining_files(self):
        """Get number of remaining .gz files to process."""
        try:
            remaining = 0
            for idx, folder in enumerate(self.folders):
                if idx < self.current_folder_idx:
                    continue
                files = self.file_cache[folder]
                if idx == self.current_folder_idx and self.current_file:
                    start_idx = files.index(self.current_file) + 1 if self.current_file in files else 0
                    remaining += len(files) - start_idx
                else:
                    existing_files = set(files).intersection(set(os.listdir(os.path.join(self.log_folder, folder))))
                    remaining += len(existing_files)
            logger.debug(f"Remaining files to process: {remaining}")
            return remaining
        except Exception as e:
            logger.error(f"Error counting remaining files: {str(e)}")
            raise

    def get_next_file(self):
        """Get the next .gz file to process."""
        try:
            while self.current_folder_idx < len(self.folders):
                current_folder = self.folders[self.current_folder_idx]
                folder_path = os.path.join(self.log_folder, current_folder)
                if not os.path.exists(folder_path):
                    logger.warning(f"Folder {current_folder} no longer exists, skipping")
                    self.current_folder_idx += 1
                    self.current_file = None
                    self.current_line = 0
                    continue
                files = sorted([f for f in os.listdir(folder_path) if f.endswith('.gz')])
                self.file_cache[current_folder] = files
                
                # Start from current_file or beginning
                start_idx = files.index(self.current_file) + 1 if self.current_file in files else 0
                
                if start_idx < len(files):
                    self.current_file = files[start_idx]
                    self.current_line = 0
                    return os.path.join(self.log_folder, current_folder, self.current_file)
                
                # Move to next folder
                self.current_folder_idx += 1
                self.current_file = None
                self.current_line = 0
            
            return None
        except Exception as e:
            logger.error(f"Error getting next file: {str(e)}")
            raise

    def process_next_file(self):
        """Process the next .gz file for aggregation and indexing (no log storage)."""
        try:
            file_path = self.get_next_file()
            if not file_path:
                logger.info("No more files to process")
                return False

            logger.info(f"Processing file: {file_path}")
            start_time = time.time()
            temp_level_counts_by_class = defaultdict(int)
            temp_level_counts_by_service = defaultdict(int)
            temp_timeline_data = defaultdict(int)
            temp_class_service_counts = defaultdict(int)
            lines_processed_in_file = 0
            skipped_lines = 0
            folder = os.path.basename(os.path.dirname(file_path))
            file_name = os.path.basename(file_path)

            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                # Skip to current line
                for _ in range(self.current_line):
                    f.readline()
                
                for line_idx, line in enumerate(f, start=self.current_line):
                    try:
                        log_entry = json.loads(line.strip())
                        log_time = log_entry.get('logtime')
                        level = log_entry.get('level')
                        class_name = log_entry.get('class')
                        
                        if not all([log_time, level, class_name]):
                            skipped_lines += 1
                            continue

                        # Validate log level
                        if level not in self.config['app']['log_levels']:
                            skipped_lines += 1
                            continue

                        # Check for log field
                        if 'log' not in log_entry:
                            logger.warning(f"Line {line_idx} missing 'log' field: {json.dumps(log_entry)[:100]}")

                        # Extract service from class
                        service = class_name.split('.')[0] if '.' in class_name else class_name
                        
                        # Parse timestamp
                        try:
                            timestamp = datetime.strptime(log_time, '%Y-%m-%d %H:%M:%S,%f')
                            rounded_timestamp = pd.Timestamp(timestamp).floor("30min")
                        except ValueError:
                            skipped_lines += 1
                            continue
                        
                        # Aggregate data (use lowercase for class to match log_index)
                        temp_level_counts_by_class[(class_name.lower(), level)] += 1
                        temp_level_counts_by_service[(service, level)] += 1
                        temp_timeline_data[(rounded_timestamp, level)] += 1
                        temp_class_service_counts[(class_name.lower(), service)] += 1
                        
                        # Update log index
                        self.log_index[(class_name.lower(), level.lower())].append((folder, file_name, line_idx))
                        self.log_index[(service.lower(), level.lower())].append((folder, file_name, line_idx))
                        
                        self.current_line = line_idx + 1
                        lines_processed_in_file += 1
                        self.total_lines_processed += 1

                    except (json.JSONDecodeError, ValueError):
                        skipped_lines += 1
                        continue

            # Update aggregated DataFrames
            if temp_level_counts_by_class:
                new_class_df = pd.DataFrame(
                    [(k[0], k[1], v) for k, v in temp_level_counts_by_class.items()],
                    columns=['class', 'level', 'count']
                )
                self.level_counts_by_class = (
                    pd.concat([self.level_counts_by_class, new_class_df])
                    .groupby(['class', 'level'])['count'].sum()
                    .reset_index()
                )
            
            if temp_level_counts_by_service:
                new_service_df = pd.DataFrame(
                    [(k[0], k[1], v) for k, v in temp_level_counts_by_service.items()],
                    columns=['service', 'level', 'count']
                )
                self.level_counts_by_service = (
                    pd.concat([self.level_counts_by_service, new_service_df])
                    .groupby(['service', 'level'])['count'].sum()
                    .reset_index()
                )
            
            if temp_timeline_data:
                new_timeline_df = pd.DataFrame(
                    [(k[0], k[1], v) for k, v in temp_timeline_data.items()],
                    columns=['timestamp', 'level', 'count']
                )
                self.timeline_data = (
                    pd.concat([self.timeline_data, new_timeline_df])
                    .groupby(['timestamp', 'level'])['count'].sum()
                    .reset_index()
                )
            
            if temp_class_service_counts:
                new_class_service_df = pd.DataFrame(
                    [(k[0], k[1], v) for k, v in temp_class_service_counts.items()],
                    columns=['class', 'service', 'count']
                )
                self.class_service_counts = (
                    pd.concat([self.class_service_counts, new_class_service_df])
                    .groupby(['class', 'service'])['count'].sum()
                    .reset_index()
                )

            self.files_processed += 1
            self.folders_processed.add(self.folders[self.current_folder_idx] if self.current_folder_idx < len(self.folders) else '')
            self.last_file_time = time.time() - start_time
            logger.info(f"Processed file: {file_path}, lines processed: {lines_processed_in_file}, lines skipped: {skipped_lines}, time taken: {self.last_file_time:.2f}s")
            return True

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

    def save_state(self):
        """Save current processing state (no logs)."""
        try:
            # Convert log_index to a flat list for faster JSON serialization
            log_index_flat = []
            for (class_or_service, level), positions in self.log_index.items():
                for folder, file_name, line_idx in positions:
                    log_index_flat.append([class_or_service, level, folder, file_name, line_idx])
            state = {
                'log_folder': self.log_folder,
                'current_folder_idx': self.current_folder_idx,
                'current_file': self.current_file,
                'current_line': self.current_line,
                'total_lines_processed': self.total_lines_processed,
                'files_processed': self.files_processed,
                'folders_processed': list(self.folders_processed),
                'log_index_flat': log_index_flat,
                'average_file_time': self.average_file_time
            }
            self.data_manager.save_data(
                self.level_counts_by_class,
                self.level_counts_by_service,
                self.timeline_data,
                self.class_service_counts
            )
            self.data_manager.save_state(state, self.log_folder)
            logger.info("Saved processor state")
        except Exception as e:
            logger.error(f"Error saving state: {str(e)}")
            raise

    def get_logs_by_class_and_level(self, class_name, level, page=1, page_size=100, search_query=None, use_regex=False):
        """Fetch all logs for a specific class and level with pagination."""
        try:
            logger.debug(f"Fetching logs for class={class_name}, level={level}, page={page}, page_size={page_size}")
            logs = []
            total_logs = 0
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size

            # If log_index is empty or incomplete, scan all files
            index_key = (class_name.lower(), level.lower())
            log_positions = self.log_index.get(index_key, [])
            if not log_positions:
                logger.debug(f"No index for {index_key}, scanning all files")
                log_positions = []
                for folder in self.folders:
                    folder_path = os.path.join(self.log_folder, folder)
                    if not os.path.exists(folder_path):
                        continue
                    files = sorted([f for f in os.listdir(folder_path) if f.endswith('.gz')])
                    for file_name in files:
                        file_path = os.path.join(folder_path, file_name)
                        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                            for line_idx, line in enumerate(f):
                                try:
                                    log_entry = json.loads(line.strip())
                                    log_class = log_entry.get('class', '')
                                    log_level = log_entry.get('level', '')
                                    if log_class.lower() == class_name.lower() and log_level.lower() == level.lower():
                                        log_positions.append((folder, file_name, line_idx))
                                except (json.JSONDecodeError, ValueError):
                                    continue
                self.log_index[index_key] = log_positions
                logger.debug(f"Built index for {index_key} with {len(log_positions)} entries")

            total_logs = len(log_positions)
            page_positions = log_positions[start_idx:end_idx]

            for folder, file_name, line_idx in page_positions:
                file_path = os.path.join(self.log_folder, folder, file_name)
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        for i, line in enumerate(f):
                            if i == line_idx:
                                try:
                                    log_entry = json.loads(line.strip())
                                    log_class = log_entry.get('class', '')
                                    if log_class.lower() != class_name.lower():
                                        total_logs -= 1
                                        break
                                    log_message = str(log_entry.get('log', '[No log message]'))
                                    timestamp = str(log_entry.get('logtime', ''))
                                    if search_query:
                                        try:
                                            if not (use_regex and (
                                                re.search(search_query, log_message, re.IGNORECASE) or
                                                re.search(search_query, timestamp, re.IGNORECASE)
                                            )) and not (not use_regex and (
                                                search_query.lower() in log_message.lower() or
                                                search_query.lower() in timestamp.lower()
                                            )):
                                                total_logs -= 1
                                                break
                                        except re.error:
                                            logger.warning(f"Invalid regex pattern: {search_query}")
                                            total_logs -= 1
                                            break
                                    logs.append({
                                        'Timestamp': timestamp,
                                        'Log Message': log_message[:1000],
                                        'Level': str(log_entry.get('level', '')),
                                        'Class': str(log_entry.get('class', ''))
                                    })
                                except (json.JSONDecodeError, ValueError):
                                    total_logs -= 1
                                    logger.warning(f"Skipped invalid log at {file_path}:{line_idx}")
                                break
                except Exception as e:
                    logger.warning(f"Error reading {file_path}:{line_idx}: {str(e)}")
                    total_logs -= 1
                if len(logs) >= page_size:
                    break

            logger.debug(f"Fetched {len(logs)} logs (page {page}, total {total_logs}) for class={class_name}, level={level}")
            return logs, total_logs
        except Exception as e:
            logger.error(f"Error fetching logs for class {class_name} and level {level}: {str(e)}")
            raise

    def get_logs_by_service_and_level(self, service_name, level, page=1, page_size=100, search_query=None, use_regex=False):
        """Fetch all logs for a specific service and level with pagination."""
        try:
            logger.debug(f"Fetching logs for service={service_name}, level={level}, page={page}, page_size={page_size}")
            logs = []
            total_logs = 0
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size

            # If log_index is empty or incomplete, scan all files
            index_key = (service_name.lower(), level.lower())
            log_positions = self.log_index.get(index_key, [])
            if not log_positions:
                logger.debug(f"No index for {index_key}, scanning all files")
                log_positions = []
                for folder in self.folders:
                    folder_path = os.path.join(self.log_folder, folder)
                    if not os.path.exists(folder_path):
                        continue
                    files = sorted([f for f in os.listdir(folder_path) if f.endswith('.gz')])
                    for file_name in files:
                        file_path = os.path.join(folder_path, file_name)
                        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                            for line_idx, line in enumerate(f):
                                try:
                                    log_entry = json.loads(line.strip())
                                    class_name = log_entry.get('class', '')
                                    log_level = log_entry.get('level', '')
                                    service = class_name.split('.')[0] if '.' in class_name else class_name
                                    if service.lower() == service_name.lower() and log_level.lower() == level.lower():
                                        log_positions.append((folder, file_name, line_idx))
                                except (json.JSONDecodeError, ValueError):
                                    continue
                self.log_index[index_key] = log_positions
                logger.debug(f"Built index for {index_key} with {len(log_positions)} entries")

            total_logs = len(log_positions)
            page_positions = log_positions[start_idx:end_idx]

            for folder, file_name, line_idx in page_positions:
                file_path = os.path.join(self.log_folder, folder, file_name)
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        for i, line in enumerate(f):
                            if i == line_idx:
                                try:
                                    log_entry = json.loads(line.strip())
                                    class_name = log_entry.get('class', '')
                                    service = class_name.split('.')[0] if '.' in class_name else class_name
                                    if service.lower() != service_name.lower():
                                        total_logs -= 1
                                        break
                                    log_message = str(log_entry.get('log', '[No log message]'))
                                    timestamp = str(log_entry.get('logtime', ''))
                                    if search_query:
                                        try:
                                            if not (use_regex and (
                                                re.search(search_query, log_message, re.IGNORECASE) or
                                                re.search(search_query, timestamp, re.IGNORECASE)
                                            )) and not (not use_regex and (
                                                search_query.lower() in log_message.lower() or
                                                search_query.lower() in timestamp.lower()
                                            )):
                                                total_logs -= 1
                                                break
                                        except re.error:
                                            logger.warning(f"Invalid regex pattern: {search_query}")
                                            total_logs -= 1
                                            break
                                    logs.append({
                                        'Timestamp': timestamp,
                                        'Log Message': log_message[:1000],
                                        'Level': str(log_entry.get('level', '')),
                                        'Class': str(log_entry.get('class', ''))
                                    })
                                except (json.JSONDecodeError, ValueError):
                                    total_logs -= 1
                                    logger.warning(f"Skipped invalid log at {file_path}:{line_idx}")
                                break
                except Exception as e:
                    logger.warning(f"Error reading {file_path}:{line_idx}: {str(e)}")
                    total_logs -= 1
                if len(logs) >= page_size:
                    break

            logger.debug(f"Fetched {len(logs)} logs (page {page}, total {total_logs}) for service={service_name}, level={level}")
            return logs, total_logs
        except Exception as e:
            logger.error(f"Error fetching logs for service {service_name} and level {level}: {str(e)}")
            raise

    def get_all_logs_by_class_and_level(self, class_name, level, search_query=None, use_regex=False):
        """Fetch all logs for a specific class and level (for download)."""
        try:
            logger.debug(f"Fetching all logs for class={class_name}, level={level}")
            logs = []
            index_key = (class_name.lower(), level.lower())
            log_positions = self.log_index.get(index_key, [])
            if not log_positions:
                logger.debug(f"No index for {index_key}, scanning all files")
                log_positions = []
                for folder in self.folders:
                    folder_path = os.path.join(self.log_folder, folder)
                    if not os.path.exists(folder_path):
                        continue
                    files = sorted([f for f in os.listdir(folder_path) if f.endswith('.gz')])
                    for file_name in files:
                        file_path = os.path.join(folder_path, file_name)
                        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                            for line_idx, line in enumerate(f):
                                try:
                                    log_entry = json.loads(line.strip())
                                    log_class = log_entry.get('class', '')
                                    log_level = log_entry.get('level', '')
                                    if log_class.lower() == class_name.lower() and log_level.lower() == level.lower():
                                        log_positions.append((folder, file_name, line_idx))
                                except (json.JSONDecodeError, ValueError):
                                    continue
                self.log_index[index_key] = log_positions

            for folder, file_name, line_idx in log_positions:
                file_path = os.path.join(self.log_folder, folder, file_name)
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        for i, line in enumerate(f):
                            if i == line_idx:
                                try:
                                    log_entry = json.loads(line.strip())
                                    log_class = log_entry.get('class', '')
                                    if log_class.lower() != class_name.lower():
                                        break
                                    log_message = str(log_entry.get('log', '[No log message]'))
                                    timestamp = str(log_entry.get('logtime', ''))
                                    if search_query:
                                        try:
                                            if not (use_regex and (
                                                re.search(search_query, log_message, re.IGNORECASE) or
                                                re.search(search_query, timestamp, re.IGNORECASE)
                                            )) and not (not use_regex and (
                                                search_query.lower() in log_message.lower() or
                                                search_query.lower() in timestamp.lower()
                                            )):
                                                break
                                        except re.error:
                                            logger.warning(f"Invalid regex pattern: {search_query}")
                                            break
                                    logs.append({
                                        'Timestamp': timestamp,
                                        'Log Message': log_message,
                                        'Level': str(log_entry.get('level', '')),
                                        'Class': str(log_entry.get('class', ''))
                                    })
                                except (json.JSONDecodeError, ValueError):
                                    logger.warning(f"Skipped invalid log at {file_path}:{line_idx}")
                                break
                except Exception as e:
                    logger.warning(f"Error reading {file_path}:{line_idx}: {str(e)}")

            logger.debug(f"Fetched {len(logs)} logs for class={class_name}, level={level}")
            return logs
        except Exception as e:
            logger.error(f"Error fetching all logs for class {class_name} and level {level}: {str(e)}")
            raise

    def get_all_logs_by_service_and_level(self, service_name, level, search_query=None, use_regex=False):
        """Fetch all logs for a specific service and level (for download)."""
        try:
            logger.debug(f"Fetching all logs for service={service_name}, level={level}")
            logs = []
            index_key = (service_name.lower(), level.lower())
            log_positions = self.log_index.get(index_key, [])
            if not log_positions:
                logger.debug(f"No index for {index_key}, scanning all files")
                log_positions = []
                for folder in self.folders:
                    folder_path = os.path.join(self.log_folder, folder)
                    if not os.path.exists(folder_path):
                        continue
                    files = sorted([f for f in os.listdir(folder_path) if f.endswith('.gz')])
                    for file_name in files:
                        file_path = os.path.join(folder_path, file_name)
                        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                            for line_idx, line in enumerate(f):
                                try:
                                    log_entry = json.loads(line.strip())
                                    class_name = log_entry.get('class', '')
                                    log_level = log_entry.get('level', '')
                                    service = class_name.split('.')[0] if '.' in class_name else class_name
                                    if service.lower() == service_name.lower() and log_level.lower() == level.lower():
                                        log_positions.append((folder, file_name, line_idx))
                                except (json.JSONDecodeError, ValueError):
                                    continue
                self.log_index[index_key] = log_positions

            for folder, file_name, line_idx in log_positions:
                file_path = os.path.join(self.log_folder, folder, file_name)
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        for i, line in enumerate(f):
                            if i == line_idx:
                                try:
                                    log_entry = json.loads(line.strip())
                                    class_name = log_entry.get('class', '')
                                    service = class_name.split('.')[0] if '.' in class_name else class_name
                                    if service.lower() != service_name.lower():
                                        break
                                    log_message = str(log_entry.get('log', '[No log message]'))
                                    timestamp = str(log_entry.get('logtime', ''))
                                    if search_query:
                                        try:
                                            if not (use_regex and (
                                                re.search(search_query, log_message, re.IGNORECASE) or
                                                re.search(search_query, timestamp, re.IGNORECASE)
                                            )) and not (not use_regex and (
                                                search_query.lower() in log_message.lower() or
                                                search_query.lower() in timestamp.lower()
                                            )):
                                                break
                                        except re.error:
                                            logger.warning(f"Invalid regex pattern: {search_query}")
                                            break
                                    logs.append({
                                        'Timestamp': timestamp,
                                        'Log Message': log_message,
                                        'Level': str(log_entry.get('level', '')),
                                        'Class': str(log_entry.get('class', ''))
                                    })
                                except (json.JSONDecodeError, ValueError):
                                    logger.warning(f"Skipped invalid log at {file_path}:{line_idx}")
                                break
                except Exception as e:
                    logger.warning(f"Error reading {file_path}:{line_idx}: {str(e)}")

            logger.debug(f"Fetched {len(logs)} logs for service={service_name}, level={level}")
            return logs
        except Exception as e:
            logger.error(f"Error fetching all logs for service {service_name} and level {level}: {str(e)}")
            raise