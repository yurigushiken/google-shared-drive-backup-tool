# drive_backup.py
import os
import io
import json
import pickle
import datetime
import re
import argparse
import glob
import time
import random
import socket
import ssl
from functools import wraps
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import httplib2
import traceback
import mimetypes
import platform
import logging
import shutil

# Long path handling for Windows
def enable_long_path_support():
    """
    Attempts to enable long path support in Windows registry.
    Requires admin privileges on Windows 10 or later.
    """
    if os.name != 'nt':  # Only applicable on Windows
        return False
        
    try:
        import winreg
        # Open the registry key
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                             r"SYSTEM\CurrentControlSet\Control\FileSystem", 
                             0, winreg.KEY_ALL_ACCESS)
        # Check if already enabled
        try:
            value, _ = winreg.QueryValueEx(key, "LongPathsEnabled")
            if value == 1:
                print("Long path support is already enabled.")
                winreg.CloseKey(key)
                return True
        except FileNotFoundError:
            pass  # Key doesn't exist yet
            
        # Set LongPathsEnabled to 1
        winreg.SetValueEx(key, "LongPathsEnabled", 0, winreg.REG_DWORD, 1)
        winreg.CloseKey(key)
        print("Long path support successfully enabled in Windows registry.")
        return True
    except Exception as e:
        print(f"Warning: Could not enable long path support in registry: {e}")
        print("Will use UNC path format instead.")
        return False

def ensure_long_path_support(path):
    """
    Converts paths to a format that supports longer than 260 characters on Windows.
    Uses the UNC '\\?\' prefix for Windows absolute paths.
    """
    if os.name != 'nt':  # Only needed on Windows
        return path
        
    # Already has UNC prefix
    if path.startswith('\\\\?\\'):
        return path
        
    # Convert to absolute path and add UNC prefix
    abs_path = os.path.abspath(path)
    return '\\\\?\\' + abs_path

# Try to enable long path support in Windows registry (requires admin)
long_path_registry_enabled = enable_long_path_support()

# Retry mechanism with exponential backoff
def retry_with_backoff(max_retries=5, initial_backoff=1, backoff_multiplier=2, max_backoff=60):
    """
    Retry decorator with exponential backoff for network operations.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_backoff = initial_backoff
            
            while True:
                try:
                    return func(*args, **kwargs)
                except (BrokenPipeError, ConnectionError, TimeoutError, 
                        httplib2.error.ServerNotFoundError, socket.error, 
                        ssl.SSLError, IOError) as e:
                    retries += 1
                    if retries > max_retries:
                        print(f"Maximum retries ({max_retries}) exceeded. Last error: {str(e)}")
                        raise
                    
                    # Add jitter to avoid thundering herd problem
                    jitter = random.uniform(0, 0.1 * current_backoff)
                    sleep_time = current_backoff + jitter
                    
                    print(f"Network error: {str(e)}. Retrying in {sleep_time:.2f} seconds (attempt {retries}/{max_retries})...")
                    time.sleep(sleep_time)
                    
                    # Increase backoff for next attempt
                    current_backoff = min(current_backoff * backoff_multiplier, max_backoff)
                    
                # Handle Google API specific errors separately
                except HttpError as e:
                    if e.resp.status == 429 or (e.resp.status == 403 and "Rate Limit Exceeded" in str(e)):
                        wait_time = 30
                        retries += 1
                        if retries > max_retries:
                            print(f"Maximum retries ({max_retries}) exceeded. Rate limit issues persist.")
                            raise
                        
                        print(f"Rate limit reached. Waiting {wait_time} seconds before retrying (attempt {retries}/{max_retries})...")
                        time.sleep(wait_time)
                    else:
                        # For other HTTP errors, just raise them
                        raise
        return wrapper
    return decorator

# Define API scopes for authentication
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Define export MIME types for Google Workspace files
GOOGLE_MIME_EXPORT_MAP = {
    'application/vnd.google-apps.document': {'mime': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'ext': 'docx'},
    'application/vnd.google-apps.spreadsheet': {'mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'ext': 'xlsx'},
    'application/vnd.google-apps.presentation': {'mime': 'application/vnd.openxmlformats-officedocument.presentationml.presentation', 'ext': 'pptx'},
    'application/vnd.google-apps.drawing': {'mime': 'image/png', 'ext': 'png'},
    'application/vnd.google-apps.script': {'mime': 'application/vnd.google-apps.script+json', 'ext': 'json'},
    'application/vnd.google-apps.jam': {'mime': 'application/pdf', 'ext': 'pdf'},
    'application/vnd.google-apps.form': {'mime': 'application/pdf', 'ext': 'pdf'}
}

# Define download chunk size (10MB)
CHUNK_SIZE = 10 * 1024 * 1024

# Get script directory for relative paths
script_dir = os.path.dirname(os.path.abspath(__file__))

# Enable UTF-8 mode for better handling of non-ASCII characters in filenames
# This is especially important on Windows
if hasattr(os, 'O_U8_DEFINED'):  # Python 3.7+ on Windows
    try:
        # Enable UTF-8 mode for Windows
        os.putenv('PYTHONIOENCODING', 'utf-8')
    except Exception as e:
        print(f"Warning: Could not enable UTF-8 mode: {e}")

# Load configuration from config.json
config_path = os.path.join(script_dir, "config.json")
with open(config_path, 'r') as f:
    config = json.load(f)

# Get shared drive ID from config
shared_drive_id = config['shared_drive_id']

# Resolve relative mirror path against script directory
mirror_root_path = os.path.join(script_dir, config['mirror_root_path'])
# Apply long path support to mirror root path
mirror_root_path = ensure_long_path_support(mirror_root_path)
print(f"Using mirror root path: {mirror_root_path}")

# Define subdirectories relative to the script directory
metadata_directory = os.path.join(script_dir, "metadata")
metadata_directory = ensure_long_path_support(metadata_directory)
reports_directory = os.path.join(script_dir, "reports")
reports_directory = ensure_long_path_support(reports_directory)

# Ensure the necessary directories exist
os.makedirs(mirror_root_path, exist_ok=True)
os.makedirs(metadata_directory, exist_ok=True)
os.makedirs(reports_directory, exist_ok=True)

def get_latest_mirror_directory():
    """Find the most recent mirror directory based on timestamp in the name."""
    mirror_dirs = glob.glob(os.path.join(mirror_root_path, "mirror-*"))
    
    if not mirror_dirs:
        return None
    
    # Sort directories by creation time (newest first)
    mirror_dirs.sort(key=lambda x: os.path.getctime(x), reverse=True)
    return ensure_long_path_support(mirror_dirs[0])

def create_new_mirror_directory():
    """Create a new timestamped mirror directory."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    mirror_dir = os.path.join(mirror_root_path, f"mirror-{timestamp}")
    # Apply long path support
    mirror_dir = ensure_long_path_support(mirror_dir)
    os.makedirs(mirror_dir, exist_ok=True)
    return mirror_dir

def save_metadata(metadata, file_path='metadata.json'):
    """
    Saves metadata to a JSON file.
    
    Args:
        metadata: Dictionary of metadata to save.
        file_path: Path to save metadata to. Default is 'metadata.json'.
    """
    try:
        logging.info(f"Attempting to save metadata to {file_path}")
        file_path = ensure_long_path_support(file_path)
        backup_path = file_path + '.bak'
        
        # Create directory for metadata file if it doesn't exist
        metadata_dir = os.path.dirname(file_path)
        if metadata_dir and not os.path.exists(metadata_dir):
            logging.info(f"Creating directory: {metadata_dir}")
            os.makedirs(metadata_dir, exist_ok=True)
        
        # If the file exists, create a backup first
        if os.path.exists(file_path):
            try:
                logging.info(f"Creating backup of existing metadata file to {backup_path}")
                shutil.copy2(file_path, backup_path)
            except Exception as e:
                logging.warning(f"Failed to create backup of metadata file: {e}")
        
        # Write the metadata to a temporary file first
        temp_file = file_path + '.tmp'
        logging.info(f"Writing metadata to temporary file: {temp_file}")
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        # Rename the temporary file to the actual file
        # This helps prevent corruption if the program is interrupted during writing
        if os.path.exists(file_path):
            logging.info(f"Removing existing metadata file: {file_path}")
            os.remove(file_path)
        logging.info(f"Renaming temporary file to final metadata file")
        os.rename(temp_file, file_path)
        
        logging.info(f"Metadata saved successfully to {file_path}")
    except Exception as e:
        logging.error(f"Error saving metadata: {e}")
        logging.error("Detailed error saving metadata:", exc_info=True)  # Add traceback logging
        if os.path.exists(backup_path):
            logging.info("Attempting to restore from backup...")
            try:
                shutil.copy2(backup_path, file_path)
                logging.info("Metadata restored from backup")
            except Exception as restore_error:
                logging.error(f"Failed to restore metadata from backup: {restore_error}")
                logging.error("Detailed restore error:", exc_info=True)  # Add traceback for restore errors too

def load_metadata(file_path='metadata.json'):
    """
    Loads metadata from a JSON file.
    
    Args:
        file_path: Path to load metadata from. Default is 'metadata.json'.
        
    Returns:
        Dictionary containing metadata, or empty dict if no metadata exists.
    """
    try:
        file_path = ensure_long_path_support(file_path)
        backup_path = file_path + '.bak'
        
        # Try to load the main file
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"Metadata loaded from {file_path}")
                return data
            except json.JSONDecodeError as e:
                print(f"Error parsing metadata file: {e}")
                
                # Try to restore from backup
                if os.path.exists(backup_path):
                    print("Attempting to restore from backup...")
                    try:
                        with open(backup_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        print("Metadata restored from backup")
                        return data
                    except Exception as backup_error:
                        print(f"Failed to load metadata from backup: {backup_error}")
        
        # If we reach here, either the file doesn't exist or we couldn't read it
        print("No existing metadata found or metadata file is corrupted. Starting fresh.")
        return {}
    except Exception as e:
        print(f"Unexpected error loading metadata: {e}")
        return {}

def save_sync_state(sync_state, file_path='changes_state.json'):
    """Persist Drive changes sync state (page tokens)."""
    try:
        save_metadata(sync_state, file_path)
        return True
    except Exception as e:
        logging.error(f"Failed to save sync state: {e}", exc_info=True)
        return False

def load_sync_state(file_path='changes_state.json'):
    """Load Drive changes sync state, defaulting to an empty dictionary."""
    data = load_metadata(file_path)
    return data if isinstance(data, dict) else {}

def is_changes_token_invalid_error(error):
    """Return True when Google rejects a stored changes page token."""
    if not isinstance(error, HttpError):
        return False
    status = getattr(error.resp, 'status', None)
    if status in (400, 404, 410):
        return True
    message = str(error).lower()
    return 'page token' in message or 'startpagetoken' in message

def sanitize_filename(filename):
    """
    Sanitize filename to ensure it's valid for all operating systems.
    Handles backslashes and other problematic characters.
    """
    # 1. Replace backslashes FIRST to avoid them being interpreted as paths
    filename = filename.replace('\\', '_')  # Replace with underscore
    
    # 2. Remove other strictly invalid characters for Windows/Unix paths
    filename = re.sub(r'[<>:"/|?*]', '', filename)
    
    # 3. Remove non-printable characters except basic whitespace
    filename = ''.join(c for c in filename if c.isprintable() or c in '\t\n\r')
    
    # 4. Strip leading/trailing whitespace
    filename = filename.strip()
    
    # 5. Windows specific: Remove trailing periods and spaces
    if os.name == 'nt':
        filename = filename.rstrip('. ')
    
    # 6. Collapse multiple internal whitespace chars
    filename = ' '.join(filename.split())
    
    # 7. Add a length limit for the filename itself
    max_filename_length = 240  # Conservative limit
    name, ext = os.path.splitext(filename)
    if len(filename.encode('utf-8')) > max_filename_length:
        # Truncate the name part, keeping the extension intact
        max_name_length = max_filename_length - len(ext.encode('utf-8'))
        name = name.encode('utf-8')[:max_name_length].decode('utf-8', 'ignore')
        name = name.rstrip('. ')  # Re-check after truncate
        filename = name + ext
    
    # 8. Handle cases where the name becomes empty after sanitization
    if not filename or filename == ext:
        return "unnamed_file" + (ext if ext else "")
    
    return filename

def sanitize_foldername(foldername):
    """
    Sanitize folder name to ensure it's valid for all operating systems.
    Handles backslashes separately to prevent Windows path misinterpretation.
    """
    # 1. Replace backslashes FIRST to avoid them being interpreted as paths
    foldername = foldername.replace('\\', '_')  # Replace with underscore
    
    # 2. Remove other strictly invalid characters for Windows/Unix paths
    foldername = re.sub(r'[<>:"/|?*]', '', foldername)
    
    # 3. Remove non-printable characters except basic whitespace
    foldername = ''.join(c for c in foldername if c.isprintable() or c in '\t\n\r')
    
    # 4. Strip leading/trailing whitespace
    foldername = foldername.strip()
    
    # 5. Windows specific: Remove trailing periods and spaces
    if os.name == 'nt':
        foldername = foldername.rstrip('. ')
    
    # 6. Collapse multiple internal whitespace chars
    foldername = ' '.join(foldername.split())
    
    # 7. Add a length limit for the component name itself
    max_component_length = 240  # Conservative limit
    if len(foldername.encode('utf-8')) > max_component_length:
        # Truncate carefully, ensuring we don't cut mid-UTF8 char
        foldername = foldername.encode('utf-8')[:max_component_length].decode('utf-8', 'ignore')
        foldername = foldername.rstrip('. ')  # Re-check after truncate
    
    # 8. Handle cases where the name becomes empty after sanitization
    if not foldername:
        return "unnamed_folder"
    
    return foldername

def get_export_mime_type(mime_type):
    """
    Get the appropriate export MIME type for Google Workspace files.
    Returns None if the MIME type is not supported for export.
    """
    export_formats = {
        'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # DOCX
        'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # XLSX
        'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # PPTX
        'application/vnd.google-apps.drawing': 'image/png',
        'application/vnd.google-apps.script': 'application/vnd.google-apps.script+json',
        'application/vnd.google-apps.jam': 'application/pdf',
        'application/vnd.google-apps.form': 'application/pdf',
        'application/vnd.google-apps.site': 'text/plain',  # No direct export support
    }
    
    # Default to PDF for supported types not explicitly listed
    if mime_type.startswith('application/vnd.google-apps.') and mime_type not in export_formats:
        # Check if it's a folder or shortcut, which can't be exported
        if mime_type in ['application/vnd.google-apps.folder', 'application/vnd.google-apps.shortcut']:
            return None
        return 'application/pdf'
        
    return export_formats.get(mime_type)

def get_extension_from_mime(mime_type):
    google_mime_map = {
        'application/vnd.google-apps.document': '.docx',
        'application/vnd.google-apps.spreadsheet': '.xlsx',
        'application/vnd.google-apps.presentation': '.pptx',
        'application/vnd.google-apps.drawing': '.png',
        'application/vnd.google-apps.script': '.json',
        'application/msword': '.doc',
        'application/vnd.ms-excel': '.xls',
        'application/vnd.ms-powerpoint': '.ppt',
        'video/quicktime': '.mov',
        'application/pdf': '.pdf',
        'text/plain': '.txt',
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'application/zip': '.zip',
        'audio/mpeg': '.mp3',
        'video/mp4': '.mp4',
        'application/vnd.google-apps.shortcut': '',  # Shortcuts will be ignored
        'application/octet-stream': ''  # For unknown binary files
    }
    return google_mime_map.get(mime_type, '')

@retry_with_backoff(max_retries=3)
def download_file(drive_service, file_id, local_path, mime_type, file_metadata):
    """
    Download a file from Google Drive.
    
    Args:
        drive_service: The authorized Google Drive service instance
        file_id: The ID of the file to download
        local_path: The local path to save the file to
        mime_type: The MIME type of the file
        file_metadata: The metadata dictionary for this file
        
    Returns:
        String indicating the result: 'downloaded', 'skipped', 'skipped_api_limit', or 'error'
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Make sure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Handle Google Docs and other exportable formats
        if mime_type.startswith('application/vnd.google-apps'):
            # Determine export format based on Google Apps type
            if mime_type in GOOGLE_MIME_EXPORT_MAP:
                export_mime = GOOGLE_MIME_EXPORT_MAP[mime_type]['mime']
                extension = GOOGLE_MIME_EXPORT_MAP[mime_type]['ext']
                
                # Update the local path with the proper extension
                base_path, _ = os.path.splitext(local_path)
                local_path = f"{base_path}.{extension}"
                
                try:
                    # Export Google Docs file
                    request = drive_service.files().export_media(fileId=file_id, mimeType=export_mime)
                    
                    # Download the file to memory first, to prevent partial files
                    file_content = io.BytesIO()
                    downloader = MediaIoBaseDownload(file_content, request, chunksize=CHUNK_SIZE)
                    
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            logger.debug(f"Download {int(status.progress() * 100)}% complete")
                    
                    # Write the content to the file
                    file_content.seek(0)
                    with open(local_path, 'wb') as f:
                        f.write(file_content.read())
                    
                    # Update metadata
                    file_metadata['local_path'] = local_path
                    file_metadata['export_mime'] = export_mime
                    
                    return 'downloaded'
                    
                except Exception as e:
                    # Some files are too large to export
                    if "Export exceeds the maximum export size" in str(e):
                        logger.warning(f"File too large to export via API: {os.path.basename(local_path)}")
                        file_metadata['error'] = f"Export size limit: {str(e)}"
                        return 'skipped_api_limit'
                    else:
                        # Other errors
                        logger.error(f"Error exporting file: {os.path.basename(local_path)} - {str(e)}")
                        file_metadata['error'] = str(e)
                        return 'error'
            else:
                # Unsupported Google Apps type
                logger.warning(f"Unsupported Google Apps type: {mime_type}")
                file_metadata['error'] = f"Unsupported Google Apps type: {mime_type}"
                return 'error'
        else:
            # Regular binary file download
            try:
                request = drive_service.files().get_media(fileId=file_id)
                
                # Download the file to memory first, to prevent partial files
                file_content = io.BytesIO()
                downloader = MediaIoBaseDownload(file_content, request, chunksize=CHUNK_SIZE)
                
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Download {int(status.progress() * 100)}% complete")
                
                # Write the content to the file
                file_content.seek(0)
                with open(local_path, 'wb') as f:
                    f.write(file_content.read())
                
                # Update metadata
                file_metadata['local_path'] = local_path
                
                return 'downloaded'
                
            except Exception as e:
                logger.error(f"Error downloading file: {os.path.basename(local_path)} - {str(e)}")
                file_metadata['error'] = str(e)
                return 'error'
                
    except Exception as e:
        logger.error(f"Error preparing download: {os.path.basename(local_path)} - {str(e)}")
        file_metadata['error'] = str(e)
        return 'error'

@retry_with_backoff(max_retries=3)
def get_total_files_and_size(drive_service, shared_drive_id):
    """
    Calculate the total number of files and total size of a shared drive.
    This is used for progress reporting and might take a while for large drives.
    
    Args:
        drive_service: Authenticated Google Drive service instance
        shared_drive_id: ID of the shared drive
        
    Returns:
        Tuple of (total_files, total_size_bytes)
    """
    print("Calculating total files and size in shared drive...")
    total_files = 0
    total_size = 0
    page_token = None
    
    try:
        while True:
            # Query all files in the shared drive (not just in root)
            response = drive_service.files().list(
                corpora="drive",
                driveId=shared_drive_id,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageToken=page_token,
                pageSize=1000,
                fields="nextPageToken, files(id, size, mimeType)"
            ).execute()
            
            items = response.get('files', [])
            
            for item in items:
                mime_type = item.get('mimeType', '')
                # Don't count folders in the file count
                if mime_type != 'application/vnd.google-apps.folder':
                    total_files += 1
                    # Add size if available
                    if 'size' in item:
                        total_size += int(item['size'])
            
            # Check if there are more pages
            page_token = response.get('nextPageToken')
            if not page_token:
                break
                
            # Print progress for large drives
            if total_files % 10000 == 0:
                print(f"Counted {total_files:,} files so far ({total_size / (1024**3):.2f} GB)...")
                
    except Exception as e:
        print(f"Warning: Error calculating total files and size: {e}")
        log_api_error(e, "calculate_totals")
        # Return default values if calculation fails
        return 0, 0
        
    return total_files, total_size

def get_errors_from_previous_report():
    """Check the most recent error report for files that failed to download."""
    try:
        # Get the most recent report file
        report_files = glob.glob(os.path.join(reports_directory, "backup_report_*.txt"))
        if not report_files:
            return []
            
        # Sort by creation time (newest first)
        report_files.sort(key=os.path.getctime, reverse=True)
        latest_report = report_files[0]
        
        # Parse the report file for error lines
        errors = []
        with open(latest_report, 'r', encoding='utf-8') as f:
            for line in f:
                if "Error" in line and "file" in line.lower():
                    # Extract the file name from the error if possible
                    errors.append(line.strip())
                    
        return errors
    except Exception as e:
        print(f"Warning: Could not check previous report for errors: {e}")
        return []

def log_api_error(error, operation, item_name='unknown'):
    """
    Log API errors using the main logger setup by setup_logging.
    This helps diagnose issues with specific files, folders, or API calls,
    keeping all logs for a single run in one file.
    
    Args:
        error: The exception object
        operation: What operation was being performed (e.g., 'list_files', 'download')
        item_name: Name of the file/folder involved (if applicable)
    """
    try:
        # Get the main logger instance
        logger = logging.getLogger(__name__) # Or just logging.getLogger() if using root
        
        # Get detailed error information
        error_type = type(error).__name__
        error_details = str(error)
        
        # For HTTP errors, extract status code
        status_code = None
        if isinstance(error, HttpError) and hasattr(error, 'resp'):
            status_code = error.resp.status
            
        # Format the log entry for clarity within the main log
        log_entry = (
            f"API Error Encountered!"
            + (f" (HTTP {status_code})" if status_code else "")
            + f"\n  Operation: {operation}"
            + f"\n  Item:      {item_name}"
            + f"\n  Type:      {error_type}"
            + f"\n  Details:   {error_details}"
        )
        
        # Log using the main logger
        logger.error(log_entry)
            
    except Exception as e:
        # If logging itself fails, print to console but don't interrupt the main process
        print(f"CRITICAL WARNING: Could not log API error via main logger: {e}")

def get_error_logs_for_date_range(start_date=None, end_date=None):
    """
    Retrieve error logs for a specified date range. - NOTE: This function relied on the separate api_errors logs. 
    It might need adjustment or removal if detailed historical error aggregation across runs is no longer needed 
    or if parsing the main logs is preferred.
    
    Args:
        start_date: Start date in 'YYYYMMDD' format or datetime object
        end_date: End date in 'YYYYMMDD' format or datetime object
        
    Returns:
        List of error log entries
    """
    try:
        errors_directory = os.path.join(script_dir, "errors")
        if not os.path.exists(errors_directory):
            return []
            
        # Convert dates to string format if they are datetime objects
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.strftime('%Y%m%d')
        if isinstance(end_date, datetime.datetime):
            end_date = end_date.strftime('%Y%m%d')
            
        # If no end date specified, use today
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y%m%d')
            
        # Get all error log files
        log_files = glob.glob(os.path.join(errors_directory, "api_errors_*.log"))
        
        # Extract dates from filenames
        log_dates = {}
        for log_file in log_files:
            filename = os.path.basename(log_file)
            # Extract date from filename (format: api_errors_YYYYMMDD.log)
            try:
                date_str = filename.split('_')[-1].split('.')[0]
                log_dates[date_str] = log_file
            except:
                continue
                
        # Filter log files by date range
        selected_logs = []
        for date_str, log_file in log_dates.items():
            if (start_date is None or date_str >= start_date) and date_str <= end_date:
                selected_logs.append(log_file)
                
        # Read content from selected log files
        all_errors = []
        for log_file in selected_logs:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_content = f.read()
                    
                # Split by double newline to get individual error entries
                error_entries = log_content.split('\n\n')
                all_errors.extend([e for e in error_entries if e.strip()])
            except Exception as e:
                print(f"Warning: Could not read error log file {log_file}: {e}")
                
        return all_errors
        
    except Exception as e:
        print(f"Warning: Error retrieving error logs: {e}")
        return []

def categorize_error_message(error_message):
    """
    Categorize an error message based on its content.
    Used for organizing errors in reports.
    
    Args:
        error_message: The error message string
    
    Returns:
        Tuple of (category_key, category_name)
    """
    error_message = error_message.lower()
    
    # Define categories and their keywords
    categories = {
        'filesystem_error': ['filesystem error', 'error-fs', 'permission denied', 'directory', 'file not found'],
        'api_export_limit': ['export size limit', 'skip-api', 'exceeds', 'api limit'],
        'network_error': ['network error', 'error-net', 'connection', 'timeout', 'connection reset', 'broken pipe'],
        'api_error': ['api error', 'error-api', 'http error', '403 forbidden', '401 unauthorized', '429 too many requests'],
        'unicode_error': ['unicode', 'encoding', 'decode', 'character'],
        'other_error': ['error']
    }
    
    # Check which category the message falls into
    for category_key, keywords in categories.items():
        if any(keyword in error_message for keyword in keywords):
            # Format the category name for display
            category_name = category_key.replace('_', ' ').title()
            return category_key, category_name
            
    # Default category
    return 'other_error', 'Other Error'

def get_mirror_creation_date(mirror_path):
    """
    Get the creation date of a mirror directory.
    Used to determine the date range for error logs.
    
    Args:
        mirror_path: Path to the mirror directory
        
    Returns:
        datetime object or None if the directory doesn't exist
    """
    try:
        if os.path.exists(mirror_path):
            # Get creation time as a timestamp
            creation_time = os.path.getctime(mirror_path)
            # Convert to datetime
            return datetime.datetime.fromtimestamp(creation_time)
        return None
    except Exception as e:
        print(f"Warning: Could not get creation date for {mirror_path}: {e}")
        return None

def parse_error_logs(error_logs):
    """
    Parse error logs into categorized errors.
    
    Args:
        error_logs: List of error log entries
        
    Returns:
        Dictionary of categorized errors
    """
    categorized_errors = {
        'filesystem_error': [],
        'api_export_limit': [],
        'network_error': [],
        'api_error': [],
        'unicode_error': [],
        'other_error': []
    }
    
    for error_entry in error_logs:
        category_key, _ = categorize_error_message(error_entry)
        categorized_errors[category_key].append(error_entry)
        
    return categorized_errors

def strip_unc_prefix(path):
    """Remove Windows UNC long-path prefix if present."""
    if isinstance(path, str) and path.startswith('\\\\?\\'):
        return path[4:]
    return path

def get_effective_local_path(file_metadata, requested_local_path):
    """
    Prefer the path written by the downloader (e.g., exported Google Docs extension),
    otherwise fall back to the requested path.
    """
    stored_local_path = file_metadata.get('local_path') if isinstance(file_metadata, dict) else None
    return stored_local_path if stored_local_path else requested_local_path

def resolve_existing_local_path(old_local_path, target_mirror_path):
    """
    Resolve metadata paths across drive-letter changes by remapping to the current
    mirror root when the mirror timestamp folder matches.
    """
    if not old_local_path:
        return old_local_path

    # Fast path: metadata path still exists as-is.
    if os.path.exists(old_local_path):
        return old_local_path

    plain_old = strip_unc_prefix(old_local_path).replace('/', '\\')
    plain_target = strip_unc_prefix(target_mirror_path).replace('/', '\\')

    mirror_pattern = re.compile(r'[\\\/](mirror-\d{8}-\d{6})(?:[\\\/](.*))?$')
    old_match = mirror_pattern.search(plain_old)
    target_match = mirror_pattern.search(plain_target)
    if not old_match or not target_match:
        return old_local_path

    old_mirror_dir = old_match.group(1).lower()
    target_mirror_dir = target_match.group(1).lower()
    if old_mirror_dir != target_mirror_dir:
        return old_local_path

    suffix = old_match.group(2)
    target_base = strip_unc_prefix(target_mirror_path)
    candidate = os.path.join(target_base, *suffix.split('\\')) if suffix else target_base

    if os.path.exists(candidate):
        return ensure_long_path_support(candidate)

    candidate_unc = ensure_long_path_support(candidate)
    if os.path.exists(candidate_unc):
        return candidate_unc

    return old_local_path

def should_calculate_drive_totals(config):
    """Return whether the expensive full-drive size pre-scan should run."""
    return bool(config.get('calculate_drive_totals_before_backup', False))

def load_config(config_path='config.json'):
    """
    Load configuration from a JSON file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Dictionary containing configuration settings
    """
    try:
        # If path is not absolute, resolve against script directory
        if not os.path.isabs(config_path):
            full_path = os.path.join(script_dir, config_path)
        else:
            full_path = config_path
            
        # Apply long path support if needed
        full_path = ensure_long_path_support(full_path)
        
        # Check if file exists
        if not os.path.exists(full_path):
            logging.warning(f"Configuration file not found: {config_path}")
            return get_default_config()
            
        # Load configuration from file
        with open(full_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        logging.info(f"Configuration loaded from {config_path}")
        
        # Return config with fallback to defaults for missing values
        default_config = get_default_config()
        for key, value in default_config.items():
            if key not in config:
                config[key] = value
                
        return config
        
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing configuration file: {e}")
        return get_default_config()
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return get_default_config()

def get_default_config():
    """
    Returns default configuration settings.
    
    Returns:
        Dictionary containing default configuration
    """
    return {
        'shared_drive_id': '',
        'mirror_root_path': 'backups',
        'report_dir': 'reports',
        'log_dir': 'logs',
        'include_shared_items': False,
        'calculate_drive_totals_before_backup': False,
        'use_changes_api_on_update': True,
        'changes_page_size': 1000
    }

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Backup Google Drive to local storage with metadata tracking')
    
    # Basic configuration
    parser.add_argument('--mode', choices=['full', 'update'], default='update',
                      help='Backup mode: full or update (default: update)')
    parser.add_argument('--include-shared', action='store_true',
                      help='Include files shared with you')
    
    # Paths
    parser.add_argument('--mirror-root-path', default=mirror_root_path,
                      help=f'Root directory for backups (default: {mirror_root_path})')
    parser.add_argument('--metadata-file', default=os.path.join(metadata_directory, 'metadata.json'),
                      help='Path to metadata file (default: metadata/metadata.json)')
    
    # Authentication
    parser.add_argument('--credentials-file', default=os.path.join(script_dir, 'credentials.json'),
                      help='Path to OAuth credentials file')
    parser.add_argument('--token-file', default=os.path.join(script_dir, 'token.pickle'),
                      help='Path to authentication token file')
    
    # Logging
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                      default='INFO', help='Logging level')
    
    return parser.parse_args()

def setup_logging(log_file):
    """
    Set up logging configuration.
    
    Args:
        log_file: The full path to the log file.
    """
    # Create logs directory if it doesn't exist (using dirname from the passed path)
    logs_directory = os.path.dirname(log_file)
    os.makedirs(logs_directory, exist_ok=True)
    
    # Get the root logger instance
    logger = logging.getLogger()  # Get the root logger instance
    logger.setLevel(logging.DEBUG)  # Changed from INFO to DEBUG for debugging
    
    # Clear existing handlers (optional, but good practice to ensure clean setup)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create Formatter
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Create and Configure File Handler
    try:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Changed from INFO to DEBUG for debugging
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)  # Add handler to the logger
    except Exception as e:
        print(f"CRITICAL: Failed to configure file logging to {log_file}: {e}")
        # Consider exiting if file logging is absolutely essential
    
    # Create and Configure Console Handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)  # Changed from INFO to DEBUG for debugging
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)  # Add handler to the logger
    
    logger.info("==== Starting backup session ====")
    logger.debug("Debug logging enabled for troubleshooting")  # Confirm debug logging is active

def authenticate(credentials_file, token_file):
    """
    Authenticate with Google Drive API.
    
    Args:
        credentials_file: Path to OAuth credentials file
        token_file: Path to token file
        
    Returns:
        Authenticated Drive API service
    """
    creds = None
    
    # Load existing token if available
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
            
    # If no valid credentials are available, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
            
        # Save the credentials for the next run
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
            
    # Build and return the Drive API service
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return service

def authenticate_drive_service():
    """
    Authenticate with Google Drive API using default credentials and token files.
    
    Returns:
        Authenticated Drive API service
    """
    credentials_file = os.path.join(script_dir, 'credentials.json')
    token_file = os.path.join(script_dir, 'token.pickle')
    
    return authenticate(credentials_file, token_file)

def get_export_format(mime_type):
    """
    Get the appropriate export format and extension for Google Workspace files.
    
    Args:
        mime_type: MIME type of the file
        
    Returns:
        Tuple of (export_mime_type, extension)
    """
    export_formats = {
        'application/vnd.google-apps.document': ('application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'docx'),
        'application/vnd.google-apps.spreadsheet': ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'xlsx'),
        'application/vnd.google-apps.presentation': ('application/vnd.openxmlformats-officedocument.presentationml.presentation', 'pptx'),
        'application/vnd.google-apps.drawing': ('image/png', 'png'),
        'application/vnd.google-apps.script': ('application/vnd.google-apps.script+json', 'json'),
        'application/vnd.google-apps.site': ('text/plain', 'txt'),
        'application/vnd.google-apps.form': ('application/pdf', 'pdf'),
        'application/vnd.google-apps.jam': ('application/pdf', 'pdf')
    }
    
    if mime_type in export_formats:
        return export_formats[mime_type]
    elif mime_type.startswith('application/vnd.google-apps.'):
        # Default to PDF for Google Workspace files not explicitly listed
        return ('application/pdf', 'pdf')
    else:
        # Not a Google Workspace file or cannot be exported
        return (None, None)

def generate_report(report_messages, report_path):
    """
    Generate a report file summarizing the backup operation.
    
    Args:
        report_messages: Dictionary containing summary, details, and error messages
        report_path: Path to save the report
    """
    try:
        # Ensure the directory exists
        report_dir = os.path.dirname(report_path)
        if report_dir and not os.path.exists(report_dir):
            os.makedirs(report_dir, exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            # Write the summary section
            f.write("===== BACKUP SUMMARY =====\n\n")
            for message in report_messages.get('summary', []):
                f.write(f"{message}\n")
            
            # Write the errors section if any
            if report_messages.get('errors', []):
                f.write("\n\n===== ERRORS AND WARNINGS (THIS SESSION) =====\n\n")
                for message in report_messages.get('errors', []):
                    f.write(f"{message}\n")
            
            # If there are errors from previous sessions, get them
            error_logs = report_messages.get('all_error_logs', [])
            if error_logs:
                f.write("\n\n===== ERRORS AND WARNINGS (ALL SESSIONS) =====\n\n")
                for error_entry in error_logs:
                    f.write(f"{error_entry}\n\n")
            
            # List files that need manual download due to API export limits
            if report_messages.get('manual_download_files', []):
                f.write("\n\n===== FILES REQUIRING MANUAL DOWNLOAD =====\n\n")
                f.write("The following files could not be exported via the API due to size limitations.\n")
                f.write("You will need to download these files manually from Google Drive:\n\n")
                
                for file_info in report_messages.get('manual_download_files', []):
                    f.write(f"* {file_info['name']} ({file_info.get('mime_type', 'Unknown type')})\n")
        
        print(f"Report saved to {report_path}")
        
    except Exception as e:
        print(f"Error generating report: {e}")
        # Attempt a very basic report if the full one fails
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"ERROR GENERATING FULL REPORT: {str(e)}\n\n")
                f.write("Basic Summary:\n")
                for message in report_messages.get('summary', []):
                    f.write(f"{message}\n")
        except:
            print("Failed to generate even a basic report.")

# Class-based implementation for Drive Backup
class DriveBackup:
    def __init__(
        self,
        drive_service,
        shared_drive_id,
        target_mirror_path,
        metadata_path,
        mode,
        report_messages,
        include_shared_items=False,
        sync_state_path=None,
        use_changes_api_on_update=True,
        changes_page_size=1000
    ):
        """
        Initialize the Drive Backup with the necessary state.
        
        Args:
            drive_service: Authenticated Google Drive service
            shared_drive_id: ID of the shared drive
            target_mirror_path: Path to the mirror directory
            metadata_path: Path to save metadata
            mode: 'full' or 'update'
            report_messages: Dictionary of message lists for the report
            include_shared_items: Whether to include shared items
            sync_state_path: Path to changes API token state file
            use_changes_api_on_update: Whether update mode should try changes.list first
            changes_page_size: changes.list page size
        """
        self.drive_service = drive_service
        self.shared_drive_id = shared_drive_id
        self.target_mirror_path = target_mirror_path
        self.metadata_path = metadata_path
        self.mode = mode
        self.report_messages = report_messages
        self.include_shared_items = include_shared_items
        self.sync_state_path = sync_state_path
        self.use_changes_api_on_update = use_changes_api_on_update
        self.changes_page_size = changes_page_size
        self.folder_path_cache = {self.shared_drive_id: ""}
        
        # Setup logging for the instance *before* using it
        self.logger = logging.getLogger(__name__)

        # Load metadata from the central path passed in metadata_path
        # The mode ('full' or 'update') affects download decisions, not which metadata file is loaded.
        self.metadata = load_metadata(self.metadata_path) 
        self.logger.info(f"Loaded metadata from central file: {self.metadata_path}")
            
        # Initialize metadata save timer
        self.next_metadata_save_time = time.time() + 300
        
        # Initialize statistics
        self.total_downloaded = 0
        self.total_skipped = 0
        self.total_errors = 0
        self.total_size = 0
        
    def save_metadata(self):
        """Save the metadata to the specified path with proper error handling."""
        try:
            self.logger.info(f"Saving metadata checkpoint to {self.metadata_path}...")
            save_metadata(self.metadata, self.metadata_path)
            self.logger.info("Metadata checkpoint saved successfully")
            self.next_metadata_save_time = time.time() + 300  # Reset timer after successful save
            return True
        except Exception as e:
            self.logger.error(f"Failed to save metadata checkpoint: {e}", exc_info=True)
            return False
    
    def download_file(self, file_id, local_path, mime_type, file_metadata):
        """
        Download a single file from Google Drive.
        Encapsulates the download_file function logic but uses instance state.
        """
        try:
            export_mime = None
            
            # Check if this is a Google Workspace file that needs exporting
            if mime_type.startswith('application/vnd.google-apps.'):
                export_mime = get_export_mime_type(mime_type)
                
                if not export_mime:
                    self.logger.info(f"Skipping folder or unsupported Google type: {os.path.basename(local_path)}")
                    return 'skipped'
                
                # Add the appropriate extension for exported files
                if mime_type in GOOGLE_MIME_EXPORT_MAP:
                    ext = GOOGLE_MIME_EXPORT_MAP[mime_type]['ext']
                    if not local_path.lower().endswith(f".{ext.lower()}"):
                        local_path = f"{local_path}.{ext}"
                
                # Try to export the file
                try:
                    request = self.drive_service.files().export_media(fileId=file_id, mimeType=export_mime)
                except HttpError as e:
                    if e.resp.status == 403 and "exportSizeLimitExceeded" in str(e):
                        file_metadata['error'] = f"Export size limit: {str(e)}"
                        return 'skipped_api_limit'
                    else:
                        file_metadata['error'] = str(e)
                        raise
                except Exception as e:
                    file_metadata['error'] = f"Unsupported Google Apps type: {mime_type}"
                    raise
                    
            else:
                # For regular files, download directly
                try:
                    request = self.drive_service.files().get_media(fileId=file_id)
                except Exception as e:
                    file_metadata['error'] = str(e)
                    raise
            
            # Create directories if they don't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download the file in chunks to handle large files
            with open(local_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request, chunksize=CHUNK_SIZE)
                done = False
                while not done:
                    try:
                        status, done = downloader.next_chunk()
                    except Exception as e:
                        file_metadata['error'] = str(e)
                        raise
            
            # Update metadata
            file_metadata['local_path'] = local_path
            file_metadata['export_mime'] = export_mime
            
            return 'downloaded'
            
        except Exception as e:
            self.logger.error(f"Error downloading: {os.path.basename(local_path)} - {str(e)}")
            file_metadata['error'] = str(e)
            return 'error'

    def _save_latest_start_page_token(self):
        """Store a fresh changes start token for future incremental update runs."""
        if not self.sync_state_path:
            return
        try:
            response = self.drive_service.changes().getStartPageToken(
                supportsAllDrives=True,
                driveId=self.shared_drive_id
            ).execute()
            new_token = response.get('startPageToken')
            if not new_token:
                return
            state = load_sync_state(self.sync_state_path)
            state['last_start_page_token'] = new_token
            state['updated_at'] = datetime.datetime.utcnow().isoformat() + 'Z'
            save_sync_state(state, self.sync_state_path)
        except Exception as e:
            self.logger.warning(f"Failed to refresh changes start token: {e}")

    def _get_folder_relative_path(self, folder_id):
        """Resolve a folder ID to a relative path under the shared drive root."""
        if not folder_id or folder_id == self.shared_drive_id:
            return ""
        if folder_id in self.folder_path_cache:
            return self.folder_path_cache[folder_id]

        try:
            folder_info = self.drive_service.files().get(
                fileId=folder_id,
                fields='id,name,mimeType,parents,trashed',
                supportsAllDrives=True
            ).execute()
        except Exception as e:
            self.logger.warning(f"Could not resolve folder path for {folder_id}: {e}")
            return ""

        if not isinstance(folder_info, dict):
            return ""
        if folder_info.get('trashed'):
            return ""

        parent_ids = folder_info.get('parents', [])
        parent_id = parent_ids[0] if parent_ids else self.shared_drive_id
        parent_rel = self._get_folder_relative_path(parent_id)
        safe_folder_name = sanitize_foldername(folder_info.get('name', 'unnamed_folder'))
        rel_path = os.path.join(parent_rel, safe_folder_name) if parent_rel else safe_folder_name
        self.folder_path_cache[folder_id] = rel_path
        return rel_path

    def _get_local_path_for_changed_file(self, file_item, mirror_folder_path):
        """Build local destination path for a changed file from its parent folder IDs."""
        file_name = file_item.get('name', 'unnamed_file')
        safe_name = sanitize_filename(file_name)
        parents = file_item.get('parents', [])
        parent_rel = self._get_folder_relative_path(parents[0]) if parents else ""

        local_dir = os.path.join(mirror_folder_path, parent_rel) if parent_rel else mirror_folder_path
        os.makedirs(local_dir, exist_ok=True)
        return os.path.join(local_dir, safe_name)

    def _process_changed_file_item(self, file_item, mirror_folder_path, prefix=""):
        """Process a single changed file entry from changes.list."""
        download_count = 0
        skipped_count = 0
        error_count = 0
        total_size = 0

        file_id = file_item.get('id')
        file_name = file_item.get('name', 'unnamed_file')
        mime_type = file_item.get('mimeType', '')

        if not file_id:
            return {'downloaded': 0, 'skipped': 1, 'errors': 0, 'total_size': 0}

        # Skip folder entries in changes feed. Folder path cache is resolved lazily.
        if mime_type == 'application/vnd.google-apps.folder':
            skipped_count += 1
            return {'downloaded': 0, 'skipped': skipped_count, 'errors': 0, 'total_size': 0}

        local_path = self._get_local_path_for_changed_file(file_item, mirror_folder_path)
        drive_modified_time = file_item.get('modifiedTime')
        drive_md5 = file_item.get('md5Checksum')
        existing_metadata = self.metadata.get(file_id)
        download_needed = True

        if existing_metadata:
            old_local_path = existing_metadata.get('local_path')
            resolved_old_local_path = resolve_existing_local_path(old_local_path, self.target_mirror_path)
            if resolved_old_local_path != old_local_path:
                self.metadata[file_id]['local_path'] = resolved_old_local_path
                old_local_path = resolved_old_local_path

            old_modified_time = existing_metadata.get('modified_time')
            if old_local_path and os.path.exists(old_local_path) and old_modified_time == drive_modified_time:
                download_needed = False
                skipped_count += 1
                self.logger.debug(f"{prefix}Skipping up-to-date changed file: {file_name}")
                self.metadata[file_id]['local_path'] = local_path
                self.metadata[file_id]['name'] = file_name
                self.metadata[file_id]['mime_type'] = mime_type

        if download_needed:
            if file_id not in self.metadata:
                self.metadata[file_id] = {}

            self.metadata[file_id]['name'] = file_name
            self.metadata[file_id]['mime_type'] = mime_type
            self.metadata[file_id]['modified_time'] = drive_modified_time
            if drive_md5:
                self.metadata[file_id]['md5Checksum'] = drive_md5

            self.logger.info(f"{prefix}Downloading changed file: {file_name}")
            try:
                result = self.download_file(file_id, local_path, mime_type, self.metadata[file_id])
                if result == 'downloaded':
                    download_count += 1
                    self.metadata[file_id]['local_path'] = get_effective_local_path(self.metadata[file_id], local_path)
                    if 'size' in file_item:
                        size = int(file_item['size'])
                        self.metadata[file_id]['size'] = size
                        total_size += size
                elif result == 'skipped_api_limit':
                    skipped_count += 1
                    self.metadata[file_id]['skipped_api_limit'] = True
                else:
                    error_count += 1
            except Exception as e:
                error_count += 1
                self.metadata[file_id]['error'] = str(e)
                self.logger.error(f"{prefix}Error downloading changed file {file_name}: {e}")
                self.report_messages['errors'].append(f"Error downloading changed file {file_name}: {e}")

        return {
            'downloaded': download_count,
            'skipped': skipped_count,
            'errors': error_count,
            'total_size': total_size
        }

    def run_update_from_changes(self, mirror_folder_path):
        """
        Run update mode using Drive changes.list token stream.
        Returns None when caller should fall back to full recursive scan.
        """
        if not self.sync_state_path:
            self.logger.info("No sync state path configured for changes API update.")
            return None

        sync_state = load_sync_state(self.sync_state_path)
        page_token = sync_state.get('last_start_page_token')
        if not page_token:
            self.logger.info("No prior changes token found; fallback to full recursive scan.")
            return None

        download_count = 0
        skipped_count = 0
        error_count = 0
        total_size = 0
        processed_count = 0

        fields = (
            "nextPageToken,newStartPageToken,"
            "changes(fileId,removed,file(id,name,mimeType,modifiedTime,md5Checksum,size,parents,trashed))"
        )

        while True:
            params = {
                'pageToken': page_token,
                'pageSize': self.changes_page_size,
                'supportsAllDrives': True,
                'includeItemsFromAllDrives': True,
                'includeRemoved': True,
                'driveId': self.shared_drive_id,
                'fields': fields
            }

            try:
                response = self.drive_service.changes().list(**params).execute()
            except Exception as e:
                if is_changes_token_invalid_error(e):
                    self.logger.warning(f"Changes token rejected by API ({e}); fallback to full recursive scan.")
                    self.report_messages['errors'].append("Changes token rejected; performed full recursive fallback.")
                    return None
                raise

            changes = response.get('changes', [])
            for change in changes:
                processed_count += 1

                if change.get('removed'):
                    file_id = change.get('fileId')
                    if file_id and file_id in self.metadata:
                        self.metadata[file_id]['removed'] = True
                        self.metadata[file_id]['removed_time'] = datetime.datetime.utcnow().isoformat() + 'Z'
                    skipped_count += 1
                    continue

                file_item = change.get('file')
                if not isinstance(file_item, dict):
                    skipped_count += 1
                    continue
                if file_item.get('trashed'):
                    skipped_count += 1
                    continue

                result = self._process_changed_file_item(file_item, mirror_folder_path)
                download_count += result.get('downloaded', 0)
                skipped_count += result.get('skipped', 0)
                error_count += result.get('errors', 0)
                total_size += result.get('total_size', 0)

                # Periodic metadata save in changes mode
                current_time = time.time()
                time_expired = current_time > self.next_metadata_save_time
                count_threshold_reached = (processed_count % 50 == 0)
                if time_expired or count_threshold_reached:
                    self.save_metadata()

            next_page_token = response.get('nextPageToken')
            if next_page_token:
                page_token = next_page_token
                continue

            new_start_page_token = response.get('newStartPageToken')
            if new_start_page_token:
                sync_state['last_start_page_token'] = new_start_page_token
                sync_state['updated_at'] = datetime.datetime.utcnow().isoformat() + 'Z'
                save_sync_state(sync_state, self.sync_state_path)
            else:
                self._save_latest_start_page_token()

            break

        self.total_downloaded += download_count
        self.total_skipped += skipped_count
        self.total_errors += error_count
        self.total_size += total_size

        return {
            'downloaded': download_count,
            'skipped': skipped_count,
            'errors': error_count,
            'total_size': total_size
        }
    
    def process_folder(self, folder_id, mirror_path, level=0, prefix="", page_token=None):
        """
        Process all files and subfolders within a folder.
        
        Args:
            folder_id: The ID of the folder to process
            mirror_path: The local path to save files
            level: Current recursion level (for logging)
            prefix: Prefix for log messages (for indentation)
            page_token: Token for pagination
            
        Returns:
            Dict with statistics: downloaded, skipped, errors, total_size
        """
        # Initialize counters for this folder
        download_count = 0
        skipped_count = 0
        error_count = 0
        total_size = 0
        
        # Create the directory if it doesn't exist
        os.makedirs(mirror_path, exist_ok=True)
        
        self.logger.info(f"{prefix}Processing folder: {os.path.basename(mirror_path)}")
        
        # Dictionary to track processed filenames to handle duplicates
        processed_names = {}
        
        # Counter for periodic metadata save
        items_processed = 0
        
        # Process files in pages
        while True:
            try:
                # Query to get all files in this folder
                query = f"'{folder_id}' in parents and trashed = false"
                
                # Fields to retrieve
                fields = "files(id, name, mimeType, size, modifiedTime, md5Checksum), nextPageToken"
                
                # Parameters for the API request
                params = {
                    'q': query,
                    'fields': fields,
                    'pageSize': 1000,
                    'supportsAllDrives': True,
                    'includeItemsFromAllDrives': True,
                }
                
                # Add shared drive context
                if self.shared_drive_id:
                    params['driveId'] = self.shared_drive_id
                    params['corpora'] = 'drive'
                
                # Add page token if available
                if page_token:
                    params['pageToken'] = page_token
                
                # Execute the list request
                self.logger.debug(f"Listing files with params: {params}")
                response = self.drive_service.files().list(**params).execute()
                
                # Check if response is valid
                if not isinstance(response, dict):
                    self.logger.error(f"Unexpected API response type: {type(response)}. Expected dictionary.")
                    error_count += 1
                    error_msg = f"API Error: Unexpected response type from Google Drive API"
                    self.logger.error(f"{prefix}{error_msg}")
                    self.report_messages['errors'].append(error_msg)
                    break
                
                # Process each file/folder
                items = response.get('files', [])
                for file_item in items:
                    items_processed += 1
                    file_id = file_item['id']
                    file_name = file_item['name']
                    mime_type = file_item['mimeType']
                    modified_time = file_item.get('modifiedTime', '')
                    
                    # Create a safe filename
                    safe_name = sanitize_filename(file_name)
                    
                    # Handle duplicate filenames
                    if safe_name in processed_names:
                        base_name, ext = os.path.splitext(safe_name)
                        counter = processed_names[safe_name] + 1
                        processed_names[safe_name] = counter
                        safe_name = f"{base_name} ({counter}){ext}"
                    else:
                        processed_names[safe_name] = 1
                    
                    # Create the local path
                    local_path = os.path.join(mirror_path, safe_name)
                    
                    # Handle folders recursively
                    if mime_type == 'application/vnd.google-apps.folder':
                        # Create the folder
                        os.makedirs(local_path, exist_ok=True)
                        
                        # Process the subfolder recursively
                        sub_results = self.process_folder(
                            file_id,
                            local_path,
                            level=level+1,
                            prefix=prefix + "  "
                        )
                        
                        # Update statistics
                        download_count += sub_results.get('downloaded', 0)
                        skipped_count += sub_results.get('skipped', 0)
                        error_count += sub_results.get('errors', 0)
                        total_size += sub_results.get('total_size', 0)
                    
                    else:
                        # Handle files
                        # Get essential info from Drive first
                        drive_modified_time = file_item.get('modifiedTime')
                        drive_md5 = file_item.get('md5Checksum') # Get checksum if available
                        
                        # Check existing metadata for this file ID
                        existing_metadata = self.metadata.get(file_id)
                        
                        download_needed = True
                        
                        # --- Update Mode Check ---
                        if self.mode == 'update' and existing_metadata:
                            old_local_path = existing_metadata.get('local_path') # Use .get for safety
                            resolved_old_local_path = resolve_existing_local_path(old_local_path, self.target_mirror_path)
                            if resolved_old_local_path != old_local_path:
                                self.metadata[file_id]['local_path'] = resolved_old_local_path
                                old_local_path = resolved_old_local_path

                            old_modified_time = existing_metadata.get('modified_time')

                            # Check if file exists locally AND modified time matches Drive's current time
                            if old_local_path and os.path.exists(old_local_path) and old_modified_time == drive_modified_time:
                                download_needed = False
                                skipped_count += 1
                                self.logger.debug(f"{prefix}Skipping up-to-date file: {file_name}") # DEBUG level

                                # Update metadata ONLY for path/name changes, preserve old mod time/checksum if skipped
                                self.metadata[file_id]['local_path'] = local_path 
                                self.metadata[file_id]['name'] = file_name
                                self.metadata[file_id]['mime_type'] = mime_type
                                # Keep old modified_time and md5Checksum in metadata when skipping
                            else:
                                # File needs download (either missing locally or modifiedTime differs)
                                self.logger.debug(f"{prefix}Detected change or missing local file for: {file_name}") # DEBUG level
                        # --- End Update Mode Check ---

                        # Update metadata fields if it's a new file or needs download
                        if download_needed:
                             # Ensure entry exists if new
                            if file_id not in self.metadata:
                                 self.metadata[file_id] = {}
                            # Store the latest info from Drive
                            self.metadata[file_id]['name'] = file_name
                            self.metadata[file_id]['mime_type'] = mime_type
                            self.metadata[file_id]['modified_time'] = drive_modified_time 
                            if drive_md5: # Store checksum if available
                                self.metadata[file_id]['md5Checksum'] = drive_md5
                            # local_path will be updated *after* successful download

                        # Download if needed
                        if download_needed:
                            self.logger.info(f"{prefix}Downloading: {file_name}") # Keep INFO for actual downloads
                            try:
                                # Download the file
                                result = self.download_file(
                                    file_id,
                                    local_path,
                                    mime_type,
                                    self.metadata[file_id]
                                )
                                
                                if result == 'downloaded':
                                    download_count += 1
                                    # Update metadata
                                    self.metadata[file_id]['local_path'] = get_effective_local_path(
                                        self.metadata[file_id],
                                        local_path
                                    )
                                    
                                    # Update size information
                                    if 'size' in file_item:
                                        size = int(file_item['size'])
                                        self.metadata[file_id]['size'] = size
                                        total_size += size
                                    
                                    # Log success
                                    self.logger.info(f"{prefix}Downloaded: {file_name}")
                                
                                elif result == 'skipped_api_limit':
                                    # File too large for API export
                                    skipped_count += 1
                                    self.metadata[file_id]['skipped_api_limit'] = True
                                    self.logger.warning(f"{prefix}API Export Size Limit: {file_name}")
                                    self.report_messages['errors'].append(f"API Export Size Limit: {file_name}")
                            
                            except Exception as e:
                                # Record error
                                error_count += 1
                                self.metadata[file_id]['error'] = str(e)
                                error_msg = f"Error downloading {file_name}: {str(e)}"
                                self.logger.error(f"{prefix}{error_msg}")
                                self.report_messages['errors'].append(error_msg)
                    
                    # Check for periodic metadata save (every 50 items or every 5 minutes)
                    current_time = time.time()
                    time_expired = current_time > self.next_metadata_save_time
                    count_threshold_reached = (items_processed % 50 == 0)
                    
                    if time_expired or count_threshold_reached:
                        self.logger.debug(f"Periodic save check: time_expired={time_expired}, count_threshold_reached={count_threshold_reached}")
                        self.save_metadata()
                
                # Get the next page token
                page_token = response.get('nextPageToken')
                
                # Continue to the next page if there is one
                if not page_token:
                    break
                
            except Exception as e:
                # Handle API errors during listing
                error_count += 1
                error_msg = f"API Error listing files in folder ID {folder_id} (local: {os.path.basename(mirror_path)}): {str(e)}"
                self.logger.error(f"{prefix}{error_msg}")
                self.report_messages['errors'].append(error_msg)
                # Log the specific error for better diagnosis
                log_api_error(e, 'list_files', item_name=f"Folder ID {folder_id}") 
                # DO NOT break here - attempt to continue if possible (e.g., if error was transient or affected only one page)
                page_token = None # Reset page token to avoid potential infinite loop if the error persists on the same page
                # Consider adding a retry mechanism specifically for listing here if needed in the future
                continue # Try the next iteration of the 'while True' loop (which might fetch the next page if page_token was updated before error)
        
        # Update aggregate statistics
        self.total_downloaded += download_count
        self.total_skipped += skipped_count
        self.total_errors += error_count
        self.total_size += total_size
        
        # Return statistics for this folder
        return {
            'downloaded': download_count,
            'skipped': skipped_count,
            'errors': error_count,
            'total_size': total_size
        }
    
    def run_backup(self):
        """Run the backup process and return statistics."""
        self.logger.info(f"Starting backup using {self.shared_drive_id}")
        
        # Get the drive name
        try:
            drive_info = self.drive_service.drives().get(driveId=self.shared_drive_id).execute()
            drive_name = drive_info.get('name', 'Unknown Drive')
            self.report_messages['summary'].append(f"Drive Name: {drive_name}")
        except Exception as e:
            self.logger.error(f"Failed to get drive info: {e}")
            drive_name = "Unknown Drive"
            self.report_messages['errors'].append(f"Failed to get drive info: {e}")
        
        # Create the mirror folder path
        mirror_folder_path = os.path.join(self.target_mirror_path, drive_name)
        
        # Create the drive folder
        try:
            os.makedirs(mirror_folder_path, exist_ok=True)
        except OSError as e:
            # Fallback if name is problematic
            safe_name = sanitize_filename(drive_name)
            mirror_folder_path = os.path.join(self.target_mirror_path, safe_name)
            os.makedirs(mirror_folder_path, exist_ok=True)
            self.report_messages['errors'].append(f"Had to sanitize drive name for folder creation: {e}")
        
        result = None

        # Prefer Drive changes API for update mode when enabled.
        if self.mode == 'update' and self.use_changes_api_on_update:
            self.logger.info("Attempting update via Drive changes API...")
            result = self.run_update_from_changes(mirror_folder_path)
            if result is not None:
                self.report_messages['summary'].append("Update Strategy: Drive changes API")
            else:
                self.report_messages['summary'].append("Update Strategy: Full recursive fallback (changes API unavailable)")

        # Fallback (or full mode): recursive folder crawl.
        if result is None:
            result = self.process_folder(
                folder_id=self.shared_drive_id,
                mirror_path=mirror_folder_path,
                level=0,
                prefix=""
            )
        
        # Final metadata save
        self.save_metadata()
        self._save_latest_start_page_token()
        
        # Return combined statistics
        return {
            'downloaded': self.total_downloaded,
            'skipped': self.total_skipped,
            'errors': self.total_errors,
            'total_size': self.total_size
        }

def main():
    """Main function to initiate the backup process."""
    try:
        # Set up command line argument parser
        parser = argparse.ArgumentParser(description='Backup Google Drive to local storage with metadata tracking')
        parser.add_argument('--config', type=str, default='config.json', help='Path to the configuration file')
        parser.add_argument('--drive-id', type=str, help='Override the Shared Drive ID from config')
        parser.add_argument('--output-dir', type=str, help='Override the output directory from config')
        parser.add_argument('--report-dir', type=str, help='Override the report directory from config')
        parser.add_argument('--log-dir', type=str, help='Override the log directory from config')

        args = parser.parse_args()

        # Load configuration
        config = load_config(args.config)
        
        # Override config with command line arguments if provided
        if args.drive_id:
            config['shared_drive_id'] = args.drive_id
        if args.output_dir:
            config['mirror_root_path'] = args.output_dir
        
        # Generate timestamp for reports and logs
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        # Set up report directory
        report_dir = args.report_dir if args.report_dir else config.get('report_dir', 'reports')
        os.makedirs(report_dir, exist_ok=True)
        
        # Generate unique report filename with timestamp
        report_filename = f"backup_report_{timestamp}.txt"
        report_path = os.path.join(report_dir, report_filename)
        
        # Set up log directory
        log_dir = args.log_dir if args.log_dir else config.get('log_dir', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Set up logging
        log_filename = f"backup_log_{timestamp}.txt"
        log_path = os.path.join(log_dir, log_filename)
        
        # Configure logging
        setup_logging(log_path)
        
        # --- Interactive Mode Selection --- 
        logger = logging.getLogger(__name__) # Get logger after setup
        
        chosen_mode = None
        while chosen_mode not in ['update', 'full']:
            try:
                prompt = "Run 'update' (use latest mirror, faster) or 'full' (create new mirror snapshot)? [update/full]: "
                user_input = input(prompt).strip().lower()
                if user_input in ['update', 'full']:
                    chosen_mode = user_input
                else:
                    print("Invalid input. Please enter 'update' or 'full'.")
            except EOFError: # Handle case where input stream is closed (e.g., non-interactive)
                 print("Non-interactive mode detected. Defaulting to 'update'.")
                 chosen_mode = 'update'
                 break 
                 
        logger.info(f"User selected mode: {chosen_mode}")
        actual_mode = chosen_mode # Use this variable hereafter
        # --- End Interactive Mode Selection ---
        
        # Get module-specific logger for this file
        # logger = logging.getLogger(__name__) # Moved up
        # logger.info(f"Starting backup in {args.mode} mode") # Use actual_mode now
        logger.info(f"Starting backup in {actual_mode} mode")
        
        # Enable long path support on Windows
        if platform.system() == 'Windows':
            enable_long_path_support()
        
        # Check for shared drive ID
        if not config.get('shared_drive_id'):
            logger.error("No shared drive ID specified in config. Cannot proceed.")
            return 1
            
        # Create and authenticate with the Drive service
        drive_service = authenticate_drive_service()
        
        # Determine the target mirror directory based on mode
        target_mirror_path = None
        mirror_root_path = config['mirror_root_path']
        
        if actual_mode == 'full':
            # For full backup, create a new timestamped directory
            target_mirror_path = create_new_mirror_directory()
            logger.info(f"Mode: Full. Created new mirror directory: {target_mirror_path}")
        else:  # mode == 'update'
            # For update, use the latest mirror directory or create a new one if none exists
            target_mirror_path = get_latest_mirror_directory()
            if target_mirror_path:
                logger.info(f"Mode: Update. Using latest mirror directory: {target_mirror_path}")
            else:
                # No existing mirror found for update, start a new baseline
                target_mirror_path = create_new_mirror_directory()
                # Clarify that this is a baseline run initiated via 'update' mode
                logger.warning(f"Mode: Update selected, but no previous mirror found. Starting new baseline in: {target_mirror_path}")
                actual_mode = 'baseline_update' # Use a distinct internal mode name for clarity if needed later
                # We will still report 'update' to the user as that's what they chose, but log the reality.
        
        if not target_mirror_path:
            logger.error("Failed to determine target mirror directory. Exiting.")
            return 1
        
        # --- Central Metadata Path --- 
        central_metadata_dir = os.path.join(script_dir, "metadata")
        os.makedirs(central_metadata_dir, exist_ok=True) # Ensure metadata directory exists
        central_metadata_path = os.path.join(central_metadata_dir, 'central_metadata.json')
        changes_sync_state_path = os.path.join(central_metadata_dir, 'changes_state.json')
        logger.info(f"Using central metadata path: {central_metadata_path}")
        logger.info(f"Using changes sync state path: {changes_sync_state_path}")
        # --- End Central Metadata Path ---
        
        # Initialize report variables
        report_messages = {
            'summary': [],
            'details': [],
            'errors': [],
            'manual_download_files': [],
            'all_error_logs': []
        }
        report_messages['summary'].append(f"Backup Report - {timestamp}")
        report_messages['summary'].append(f"Mode Selected: {chosen_mode}") # Report what the user chose
        if chosen_mode == 'update' and actual_mode == 'baseline_update':
             report_messages['summary'].append(f"(Note: No previous mirror found, created new baseline)")
        report_messages['summary'].append(f"Mirror Directory: {target_mirror_path}")
        
        # Start the backup process
        start_time = time.time()
        
        # Optional: expensive full-drive pre-scan for progress percentages.
        total_files_in_drive = 0
        total_size_in_drive = 0
        if should_calculate_drive_totals(config):
            total_files_in_drive, total_size_in_drive = get_total_files_and_size(drive_service, config['shared_drive_id'])
            if total_files_in_drive > 0:
                report_messages['summary'].append(f"Total files in drive: {total_files_in_drive:,}")
            if total_size_in_drive > 0:
                total_size_gb = total_size_in_drive / (1024 ** 3)
                report_messages['summary'].append(f"Total size of drive: {total_size_gb:.2f} GB")
        else:
            logger.info("Skipping full-drive totals pre-scan (calculate_drive_totals_before_backup=false).")
        
        # Get error logs from previous sessions
        mirror_creation_date = get_mirror_creation_date(target_mirror_path)
        if mirror_creation_date:
            # Get error logs since the mirror was created
            all_error_logs = get_error_logs_for_date_range(start_date=mirror_creation_date)
            report_messages['all_error_logs'] = all_error_logs
        
        # Create the drive backup instance
        backup_instance = DriveBackup(
            drive_service=drive_service,
            shared_drive_id=config['shared_drive_id'],
            target_mirror_path=target_mirror_path,
            metadata_path=central_metadata_path,
            mode=actual_mode,
            report_messages=report_messages,
            include_shared_items=config.get('include_shared_items', False),
            sync_state_path=changes_sync_state_path,
            use_changes_api_on_update=config.get('use_changes_api_on_update', True),
            changes_page_size=config.get('changes_page_size', 1000)
        )
        
        # Run the backup
        try:
            result = backup_instance.run_backup()
            
            # Calculate elapsed time
            end_time = time.time()
            elapsed_time = end_time - start_time
            hours, remainder = divmod(elapsed_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Add summary to report
            report_messages['summary'].append(f"\nBackup completed in {int(hours)}h {int(minutes)}m {int(seconds)}s")
            report_messages['summary'].append(f"Total files downloaded: {result['downloaded']}")
            report_messages['summary'].append(f"Total files skipped: {result['skipped']}")
            report_messages['summary'].append(f"Total files with errors: {result['errors']}")
            
            # Add size information
            if result['total_size'] > 0:
                size_gb = result['total_size'] / (1024 ** 3)
                report_messages['summary'].append(f"Total data downloaded: {size_gb:.2f} GB")
                
                # Calculate percentage if total drive size is known
                if total_size_in_drive > 0:
                    percentage = (result['total_size'] / total_size_in_drive) * 100
                    report_messages['summary'].append(f"Estimated percentage of Shared Drive downloaded: {percentage:.2f}%")
                    # Add note about potential >100% due to retries/updates
                    if result['total_size'] > total_size_in_drive:
                         report_messages['summary'].append("  (Note: Downloaded size may exceed total drive size if files were re-downloaded due to previous errors or updates.)")
            
            # Identify files requiring manual download
            manual_download_files = []
            for file_id, file_data in backup_instance.metadata.items():
                if file_data.get('skipped_api_limit') and 'name' in file_data:
                    manual_download_files.append({
                        'name': file_data['name'],
                        'mime_type': file_data.get('mime_type', 'Unknown type'),
                        'id': file_id
                    })
            
            if manual_download_files:
                report_messages['manual_download_files'] = manual_download_files
                report_messages['summary'].append(f"Files requiring manual download: {len(manual_download_files)}")
            
            # Generate the report
            generate_report(report_messages, report_path)
            
            logger.info(f"Backup completed. Report saved to {report_path}")
            return 0
            
        except Exception as e:
            logger.exception(f"Error during backup process: {e}")
            report_messages['errors'].append(f"Error during backup process: {e}")
            
            # Try to save metadata even if there was an error
            if backup_instance:
                backup_instance.save_metadata()
            else:
                logger.error("Backup instance not created, cannot save metadata.")
            
            # Generate an error report
            generate_report(report_messages, report_path)
            return 1
        
    except Exception as e:
        logging.exception(f"Unhandled exception in main: {e}")
        print(f"An error occurred: {e}")
        print("Check the log file for details")
        return 1

if __name__ == "__main__":
    main() 
