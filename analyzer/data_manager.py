import pandas as pd
import os
import json
import logging
from io import BytesIO
import openpyxl
import glob
from datetime import datetime

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self, config):
        """Initialize data manager with configuration."""
        self.config = config
        self.data_dir = config['paths']['data_dir']
        self.state_dir = config['paths']['state_dir']
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.state_dir, exist_ok=True)
        self.level_counts_by_class_file = os.path.join(self.data_dir, 'level_counts_by_class.csv')
        self.level_counts_by_service_file = os.path.join(self.data_dir, 'level_counts_by_service.csv')
        self.timeline_file = os.path.join(self.data_dir, 'timeline_data.csv')
        self.class_service_file = os.path.join(self.data_dir, 'class_service_counts.csv')

    def clear_data(self):
        """Clear all processed data files for a fresh run."""
        try:
            csv_files = glob.glob(os.path.join(self.data_dir, '*.csv'))
            for file in csv_files:
                os.remove(file)
                logger.info(f"Deleted data file: {file}")
            state_files = glob.glob(os.path.join(self.state_dir, '*.json'))
            for file in state_files:
                os.remove(file)
                logger.info(f"Deleted state file: {file}")
        except Exception as e:
            logger.error(f"Error clearing data: {str(e)}")
            raise

    def save_data(self, level_counts_by_class, level_counts_by_service, timeline_data, class_service_counts):
        """Save analysis data to CSV files."""
        try:
            level_counts_by_class.to_csv(self.level_counts_by_class_file, index=False)
            level_counts_by_service.to_csv(self.level_counts_by_service_file, index=False)
            timeline_data.to_csv(self.timeline_file, index=False)
            class_service_counts.to_csv(self.class_service_file, index=False)
            logger.info("Saved analysis data to CSV files")
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            raise

    def load_raw_level_counts_by_class(self):
        """Load raw level counts by class from CSV."""
        try:
            if os.path.exists(self.level_counts_by_class_file):
                df = pd.read_csv(self.level_counts_by_class_file)
                df = df.astype({'count': 'int64'})
                logger.debug(f"Loaded raw level_counts_by_class: shape={df.shape}")
                return df
            logger.debug("No level_counts_by_class CSV found")
            return pd.DataFrame(columns=['class', 'level', 'count']).astype({'count': 'int64'})
        except Exception as e:
            logger.error(f"Error loading raw level_counts_by_class: {str(e)}")
            raise

    def load_raw_level_counts_by_service(self):
        """Load raw level counts by service from CSV."""
        try:
            if os.path.exists(self.level_counts_by_service_file):
                df = pd.read_csv(self.level_counts_by_service_file)
                df = df.astype({'count': 'int64'})
                logger.debug(f"Loaded raw level_counts_by_service: shape={df.shape}")
                return df
            logger.debug("No level_counts_by_service CSV found")
            return pd.DataFrame(columns=['service', 'level', 'count']).astype({'count': 'int64'})
        except Exception as e:
            logger.error(f"Error loading raw level_counts_by_service: {str(e)}")
            raise

    def load_raw_timeline_data(self):
        """Load raw timeline data from CSV."""
        try:
            if os.path.exists(self.timeline_file):
                df = pd.read_csv(self.timeline_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.astype({'count': 'int64'})
                logger.debug(f"Loaded raw timeline_data: shape={df.shape}")
                return df
            logger.debug("No timeline_data CSV found")
            return pd.DataFrame(columns=['timestamp', 'level', 'count']).astype({'count': 'int64'})
        except Exception as e:
            logger.error(f"Error loading raw timeline_data: {str(e)}")
            raise

    def load_raw_class_service_counts(self):
        """Load raw class/service counts from CSV."""
        try:
            if os.path.exists(self.class_service_file):
                df = pd.read_csv(self.class_service_file)
                df = df.astype({'count': 'int64'})
                logger.debug(f"Loaded raw class_service_counts: shape={df.shape}")
                return df
            logger.debug("No class_service_counts CSV found")
            return pd.DataFrame(columns=['class', 'service', 'count']).astype({'count': 'int64'})
        except Exception as e:
            logger.error(f"Error loading raw class_service_counts: {str(e)}")
            raise

    def get_level_counts_by_class(self):
        """Get aggregated log level counts by class."""
        try:
            if os.path.exists(self.level_counts_by_class_file):
                df = pd.read_csv(self.level_counts_by_class_file)
                pivot = pd.pivot_table(
                    df,
                    values='count',
                    index='class',
                    columns='level',
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                logger.debug(f"Level counts by class: {pivot.shape}")
                return pivot
            logger.debug("No level counts by class data available")
            return pd.DataFrame(columns=['class'] + self.config['app']['log_levels'])
        except Exception as e:
            logger.error(f"Error getting level counts by class: {str(e)}")
            raise

    def get_level_counts_by_service(self):
        """Get aggregated log level counts by service."""
        try:
            if os.path.exists(self.level_counts_by_service_file):
                df = pd.read_csv(self.level_counts_by_service_file)
                pivot = pd.pivot_table(
                    df,
                    values='count',
                    index='service',
                    columns='level',
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                logger.debug(f"Level counts by service: {pivot.shape}")
                return pivot
            logger.debug("No level counts by service data available")
            return pd.DataFrame(columns=['service'] + self.config['app']['log_levels'])
        except Exception as e:
            logger.error(f"Error getting level counts by service: {str(e)}")
            raise

    def get_timeline_data(self):
        """Get timeline data for log levels."""
        try:
            if os.path.exists(self.timeline_file):
                df = pd.read_csv(self.timeline_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                aggregated = df.groupby(['timestamp', 'level']).sum().reset_index()
                logger.debug(f"Timeline data: {aggregated.shape}")
                return aggregated
            logger.debug("No timeline data available")
            return pd.DataFrame(columns=['timestamp', 'level', 'count'])
        except Exception as e:
            logger.error(f"Error getting timeline data: {str(e)}")
            raise

    def get_class_service_counts(self):
        """Get total counts per class and service."""
        try:
            if os.path.exists(self.class_service_file):
                df = pd.read_csv(self.class_service_file)
                aggregated = df.groupby(['class', 'service']).sum().reset_index()
                logger.debug(f"Class/Service counts: {aggregated.shape}")
                return aggregated
            logger.debug("No class/service counts available")
            return pd.DataFrame(columns=['class', 'service', 'count'])
        except Exception as e:
            logger.error(f"Error getting class/service counts: {str(e)}")
            raise

    def save_state(self, state, log_folder):
        """Save analysis state with folder_path_datetime naming."""
        try:
            # Create filename: replace '/' with '_' in folder path and append datetime
            folder_part = log_folder.replace('/', '_').replace('\\', '_')
            dt_part = datetime.now().strftime('%Y%m%d%H%M%S')
            state_filename = f"{folder_part}_{dt_part}.json"
            state_path = os.path.join(self.state_dir, state_filename)
            with open(state_path, 'w') as f:
                json.dump(state, f)
            logger.info(f"Saved state to {state_path}")
            # Clean up old states (keep only latest 10)
            state_files = sorted(glob.glob(os.path.join(self.state_dir, '*.json')), key=os.path.getmtime, reverse=True)
            for old_file in state_files[10:]:
                os.remove(old_file)
                logger.info(f"Deleted old state file: {old_file}")
        except Exception as e:
            logger.error(f"Error saving state: {str(e)}")
            raise

    def load_state(self, state_file):
        """Load analysis state from a specific file."""
        try:
            if os.path.exists(state_file):
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    logger.info(f"Loaded state from {state_file}")
                    return state
            logger.debug(f"State file {state_file} not found")
            return None
        except Exception as e:
            logger.error(f"Error loading state from {state_file}: {str(e)}")
            raise

    def create_excel(self):
        """Create an Excel file with multiple sheets for analysis results."""
        try:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                self.get_level_counts_by_class().to_excel(writer, sheet_name='Level_Counts_by_Class', index=False)
                self.get_level_counts_by_service().to_excel(writer, sheet_name='Level_Counts_by_Service', index=False)
                self.get_timeline_data().to_excel(writer, sheet_name='Timeline_Data', index=False)
                self.get_class_service_counts().to_excel(writer, sheet_name='Class_Service_Counts', index=False)
            buffer.seek(0)
            logger.info("Created Excel file for download")
            return buffer
        except Exception as e:
            logger.error(f"Error creating Excel file: {str(e)}")
            raise