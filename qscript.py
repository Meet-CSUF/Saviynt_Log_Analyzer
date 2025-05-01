import os
import gzip
import json
from collections import defaultdict
import pandas as pd
from datetime import datetime
import dateutil.parser
from tqdm import tqdm

def get_valid_folder_path():
    while True:
        folder_path = input("Please enter the path to your logs folder: ").strip()
        
        if not folder_path:
            print("Path cannot be empty. Please try again.")
            continue
            
        folder_path = os.path.expanduser(folder_path)
        folder_path = os.path.abspath(folder_path)
        
        if not os.path.exists(folder_path):
            print(f"The path '{folder_path}' does not exist. Please try again.")
            continue
        if not os.path.isdir(folder_path):
            print(f"The path '{folder_path}' is not a directory. Please try again.")
            continue
            
        try:
            os.listdir(folder_path)
        except PermissionError:
            print(f"Permission denied: Cannot access '{folder_path}'")
            continue
            
        return folder_path

def count_gz_files(base_folder):
    """Count total .gz files to process"""
    total_files = 0
    for root, _, files in os.walk(base_folder):
        total_files += sum(1 for f in files if f.endswith('.gz'))
    return total_files

def analyze_logs(base_folder):
    log_entries = []
    files_processed = 0
    total_lines = 0
    error_lines = 0
    
    # Count total files first
    total_files = count_gz_files(base_folder)
    print(f"\nFound {total_files} .gz files to process")
    
    # Create progress bars
    file_progress = tqdm(total=total_files, desc="Processing files", unit="file")
    
    for root, _, files in os.walk(base_folder):
        gz_files = [f for f in files if f.endswith('.gz')]
        for file in gz_files:
            file_path = os.path.join(root, file)
            try:
                current_file_lines = 0
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        total_lines += 1
                        current_file_lines += 1
                        try:
                            log_data = json.loads(line.strip())
                            log_entries.append({
                                'timestamp': log_data.get('logtime'),
                                'thread': log_data.get('thread'),
                                'level': log_data.get('level'),
                                'class': log_data.get('class'),
                                'message': log_data.get('log'),
                                'container': log_data.get('kubernetes', {}).get('container_name'),
                                'namespace': log_data.get('kubernetes', {}).get('namespace_name'),
                                'pod': log_data.get('kubernetes', {}).get('pod_name'),
                                'host': log_data.get('kubernetes', {}).get('host')
                            })
                        except json.JSONDecodeError:
                            error_lines += 1
                
                files_processed += 1
                file_progress.update(1)
                file_progress.set_postfix({
                    'Lines': total_lines, 
                    'Errors': error_lines,
                    'Current File Lines': current_file_lines
                })
                
            except Exception as e:
                print(f"\nError processing file {file_path}: {str(e)}")
                error_lines += 1
                file_progress.update(1)
    
    file_progress.close()
    
    # Print final statistics
    print("\nProcessing Complete!")
    print(f"Files Processed: {files_processed}/{total_files}")
    print(f"Total Lines: {total_lines}")
    print(f"Successful Lines: {len(log_entries)}")
    print(f"Error Lines: {error_lines}")
    
    return log_entries, files_processed, total_lines, error_lines

def parse_timestamp(timestamp_str):
    try:
        return dateutil.parser.parse(timestamp_str)
    except:
        try:
            return datetime.strptime(timestamp_str, "%d/%b/%Y:%H:%M:%S +0000")
        except:
            return None

def generate_analysis(log_entries):
    print("\nGenerating analysis...")
    
    # Convert to DataFrame
    df = pd.DataFrame(log_entries)
    
    # Convert timestamps with progress bar
    print("Processing timestamps...")
    tqdm.pandas(desc="Parsing timestamps")
    df['parsed_timestamp'] = df['timestamp'].progress_apply(parse_timestamp)
    df['hour'] = df['parsed_timestamp'].dt.hour
    
    print("Creating aggregations...")
    analysis = {
        'class_level_counts': pd.pivot_table(
            df, 
            index='class', 
            columns='level', 
            aggfunc='size', 
            fill_value=0
        ).reset_index(),
        
        'level_summary': df['level'].value_counts().reset_index(),
        
        'class_summary': df['class'].value_counts().reset_index(),
        
        'pod_summary': df['pod'].value_counts().reset_index(),
        
        'container_summary': df['container'].value_counts().reset_index(),
        
        'host_summary': df['host'].value_counts().reset_index(),
        
        'class_level_pod': pd.pivot_table(
            df,
            index=['class', 'pod'],
            columns='level',
            aggfunc='size',
            fill_value=0
        ).reset_index(),
        
        'hourly_level_counts': pd.pivot_table(
            df,
            index='hour',
            columns='level',
            aggfunc='size',
            fill_value=0
        ).reset_index().sort_values('hour'),
        
        # Add thread analysis
        'thread_summary': df['thread'].value_counts().reset_index(),
        
        # Add class and level combinations with high error counts
        'error_analysis': df[df['level'] == 'ERROR'].groupby(['class', 'pod'])['level'].count().reset_index().sort_values('level', ascending=False)
    }
    
    if len(df) > 0:
        analysis['time_range'] = pd.DataFrame([{
            'start_time': df['parsed_timestamp'].min(),
            'end_time': df['parsed_timestamp'].max(),
            'duration_hours': (df['parsed_timestamp'].max() - df['parsed_timestamp'].min()).total_seconds() / 3600
        }])
    
    return analysis

def save_to_csv(analysis, base_folder):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = os.path.join(base_folder, 'log_analysis_output')
    os.makedirs(output_dir, exist_ok=True)
    
    print("\nSaving analysis files...")
    for name, data in tqdm(analysis.items(), desc="Saving files"):
        filename = os.path.join(output_dir, f'{name}_{timestamp}.csv')
        data.to_csv(filename, index=False)
        print(f"Saved {name} to {filename}")

def print_summary(analysis, total_lines, error_lines):
    print("\nLog Analysis Summary")
    print("-" * 80)
    
    if 'time_range' in analysis and not analysis['time_range'].empty:
        time_range = analysis['time_range'].iloc[0]
        print("\nTime Range:")
        print(f"Start: {time_range['start_time']}")
        print(f"End: {time_range['end_time']}")
        print(f"Duration: {time_range['duration_hours']:.2f} hours")
    
    print("\nLog Level Distribution:")
    level_counts = analysis['level_summary']
    total_logs = level_counts[0].sum()
    for _, row in level_counts.iterrows():
        percentage = (row[0] / total_logs) * 100
        print(f"{row['level']:<10}: {row[0]:>6} ({percentage:>6.2f}%)")
    
    print("\nTop 5 Classes by Log Count:")
    class_summary = analysis['class_summary'].head()
    for _, row in class_summary.iterrows():
        print(f"{row['class']:<40}: {row[0]:>6}")
    
    print("\nTop 5 Pods by Log Count:")
    pod_summary = analysis['pod_summary'].head()
    for _, row in pod_summary.iterrows():
        print(f"{row['pod']:<40}: {row[0]:>6}")
    
    print("\nTop 5 Classes with Errors:")
    error_summary = analysis['error_analysis'].head()
    for _, row in error_summary.iterrows():
        print(f"{row['class']:<40} ({row['pod']}): {row['level']:>6}")
    
    print("\nHourly Distribution:")
    hourly_counts = analysis['hourly_level_counts']
    for _, row in hourly_counts.iterrows():
        total = sum(row[1:])
        print(f"Hour {row['hour']:02d}: {total:>6} logs")
    
    print("\nProcessing Statistics:")
    print(f"Total lines processed: {total_lines}")
    print(f"Successfully parsed lines: {total_lines - error_lines}")
    print(f"Failed to parse lines: {error_lines}")
    if total_lines > 0:
        success_rate = ((total_lines - error_lines) / total_lines) * 100
        print(f"Success rate: {success_rate:.2f}%")

def main():
    print("Log Analysis Tool")
    print("----------------")
    
    try:
        folder_path = get_valid_folder_path()
        print(f"\nAnalyzing logs in: {folder_path}")
        
        log_entries, files_processed, total_lines, error_lines = analyze_logs(folder_path)
        
        if not log_entries:
            print("\nNo log entries found. Exiting.")
            return
        
        analysis = generate_analysis(log_entries)
        save_to_csv(analysis, folder_path)
        print_summary(analysis, total_lines, error_lines)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {str(e)}")
        raise e  # This will show the full error traceback

if __name__ == "__main__":
    main()
