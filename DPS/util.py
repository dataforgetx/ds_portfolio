"""
Utility functions for DPS workflow scripts.

This module contains shared functions for database connections, configuration,
logging, and email notifications that are used by both send2dps.py and receivedps.py.
"""

import logging
import os
import shutil
import smtplib
import stat
from datetime import date, datetime
from email.message import EmailMessage
from logging.handlers import RotatingFileHandler
from pathlib import Path
from subprocess import check_output, CalledProcessError

import cx_Oracle
import paramiko
import yaml


def get_password(user, db):
    """
    Retrieve database password using 1Password CLI.

    Args:
        user (str): Database username
        db (str): Database name

    Returns:
        str: Database password
    """
    cmd = f'/usr/bin/op -g dba {user}@{db}'
    password = check_output(cmd, shell=True).decode().rstrip()
    return password


def _expand_paths(obj):
    """
    Recursively expand ~ in string values that look like paths.

    Args:
        obj: Dictionary, list, or string to process

    Returns:
        Object with ~ expanded in path-like strings
    """
    if isinstance(obj, dict):
        return {key: _expand_paths(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_expand_paths(item) for item in obj]
    elif isinstance(obj, str) and obj.startswith('~'):
        return os.path.expanduser(obj)
    else:
        return obj


def read_config(env, config_file='dps_config.yml'):
    """
    Read YAML configuration file for environment-specific settings.
    Expands ~ in path values to user home directory.

    Args:
        env (str): Environment name (dev, prod, qawh)
        config_file (str): Path to config file (default: dps_config.yml)

    Returns:
        tuple: (config dict, envconfig dict)
    """
    # Expand ~ in config file path itself
    config_file = os.path.expanduser(config_file)

    with open(config_file, 'r') as file:
        # loads the YAML file into a Python dictionary
        config = yaml.safe_load(file)

    # Expand ~ in all path values to the user's home directory
    config = _expand_paths(config)

    return config['config'], config['env'][env]


def read_config_from_script_dir(env, config_filename='dps_config_test.yml'):
    """
    Read configuration file from the same directory as the calling script.

    This is a convenience function that automatically locates the config file
    relative to the script that calls it, eliminating duplicated config reading code.

    Args:
        env (str): Environment name (dev, prod, qawh)
        config_filename (str): Name of config file (default: dps_config_test.yml)

    Returns:
        tuple: (config dict, envconfig dict)
    """
    import inspect
    # Get the frame of the caller (the script that called this function)
    frame = inspect.stack()[1]
    script_path = Path(frame.filename)
    script_dir = script_path.parent
    config_file = script_dir / config_filename
    return read_config(env, config_file=str(config_file))


def get_dbconnection(envconfig):
    """
    Establish connection to Oracle database using credentials from environment.

    Args:
        envconfig (dict): Environment configuration dictionary

    Returns:
        cx_Oracle.Connection: Database connection object
    """
    db = envconfig['db']
    user = envconfig['dbuser']
    # pwd = get_password(user, db)
    pwd = envconfig['password']
    connection = cx_Oracle.connect(user=user, password=pwd, dsn=db)
    return connection


def get_logger(log_name, log_file, log_dir):
    """
    Initialize and configure logger.

    Args:
        log_name (str): Name for the logger
        log_file (str): Log filename (without path)
        log_dir (str): Directory for log files

    Returns:
        logging.Logger: Configured logger instance
    """
    # create a logger instance
    log = logging.getLogger(log_name)
    # levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
    # only INFO and above will be logged
    log.setLevel(logging.INFO)

    # Create log directory if it doesn't exist
    # Expand ~ if present (defensive - should already be expanded from config)
    log_dir = os.path.expanduser(str(log_dir))
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    log_file_path = f'{log_dir}/{log_file}'

   # rotating file handler means that if the log file exceeds the maximum size,
   # a new log file is created
    handler = RotatingFileHandler(
        filename=log_file_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB is the maximum size of the log file
        backupCount=5  # 5 backup files will be kept
    )

    formatter = logging.Formatter("%(asctime)s - %(levelname)8s - %(message)s")
    handler.setFormatter(formatter)

   # adds the handler to the logger
    log.addHandler(handler)
    return log


def email_error(message, error_email, script_name="DPS Data Share Script", filename=None):
    """
    Send error notification email.

    Args:
        message (str): Error message
        error_email (str or list): Email address(es) to send error to
        script_name (str): Name of script generating error
        filename (str, optional): Log file to attach
    """
    msg = EmailMessage()
    msg.set_content(message)
    msg["Subject"] = f"ERROR from {script_name}"
    msg["From"] = "noreply@dfps.texas.gov"
    # Handle both single email address and list of email addresses
    if isinstance(error_email, list):
        msg["To"] = ", ".join(error_email)
    else:
        msg["To"] = error_email

    if filename:
        with open(filename, "rb") as f:
            file_data = f.read()
            msg.add_attachment(file_data, maintype="text",
                               subtype="plain", filename=filename)

    try:
        with smtplib.SMTP("localhost", 25) as server:
            server.send_message(msg)
    except Exception as e:
        # If email fails, at least log it
        print(f"Failed to send error email: {e}")


def send_notification_email(recipient_emails, subject, message, logger=None):
    """
    Send notification email without attachments.

    Generic function to send notification emails. Recipient emails can be a list or comma-separated string.

    Args:
        recipient_emails (str or list): Email address(es) to send notification to
        subject (str): Email subject line
        message (str): Email body message
        logger: Logger object for logging (optional)

    Raises:
        ValueError: If recipient_emails is empty or invalid
        Exception: If email sending fails
    """
    # Handle recipient emails - can be list, comma-separated string, or single string
    if isinstance(recipient_emails, list):
        recipient_list = recipient_emails
    elif isinstance(recipient_emails, str):
        # Check if it's already comma-separated or single email
        if ',' in recipient_emails:
            recipient_list = [email.strip()
                              for email in recipient_emails.split(',')]
        else:
            recipient_list = [recipient_emails.strip()]
    else:
        recipient_list = []

    # Validate recipient emails
    # Remove empty strings
    recipient_list = [email for email in recipient_list if email]
    if not recipient_list:
        error_msg = "No valid recipient emails provided"
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg)

    # Send email notification (no attachments)
    try:
        msg = EmailMessage()
        msg.set_content(message)
        msg["Subject"] = subject
        msg["From"] = "noreply@dfps.texas.gov"
        msg["To"] = ", ".join(recipient_list)
        with smtplib.SMTP("localhost", 25) as server:
            server.send_message(msg)
        if logger:
            logger.info(
                f"Sent notification email to {', '.join(recipient_list)}")
    except Exception as e:
        error_msg = f"Failed to send notification email: {e}"
        if logger:
            logger.error(error_msg)
        raise Exception(error_msg)


def get_sftp_connection(configs):
    """
    Establish SFTP connection to DPS server.

    Args:
        configs (dict): Configuration dictionary with SFTP settings

    Returns:
        tuple: (sftp, ssh) - SFTP client and SSH client connections

    Raises:
        KeyError: If required SFTP configuration keys are missing
        Exception: If SFTP connection fails (authentication, network, or other errors)
    """
    try:
        sftp_config = configs['sftp']
        hostname = sftp_config['hostname']
        username = sftp_config['username']
        password = sftp_config['password']
    except KeyError as e:
        raise KeyError(f"Missing required SFTP configuration: {e}") from e

    # creates an SSH client object
    ssh = None
    sftp = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # connects to the SSH server
        ssh.connect(hostname, username=username, password=password, timeout=30)
        # open an SFTP session over the SSH connection
        sftp = ssh.open_sftp()

        return sftp, ssh  # Return both to keep connection alive
    except Exception as e:
        # Clean up on any error (authentication, connection, or other)
        # Close SFTP first (if it exists), then SSH
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass  # Ignore errors when closing failed connection
        if ssh:
            try:
                ssh.close()
            except Exception:
                pass  # Ignore errors when closing failed connection
        raise Exception(
            f"Failed to establish SFTP connection to {hostname} for {username}: {e}") from e


def upload_file_sftp(sftp, local_path, remote_dir, remote_filename=None):
    """
    Upload file to SFTP server.

    Args:
        sftp: SFTP client connection
        local_path (str or Path): Local file path to upload
        remote_dir (str): Remote directory path
        remote_filename (str, optional): Remote filename (defaults to local filename)

    Returns:
        str: Remote file path

    Raises:
        FileNotFoundError: If local file doesn't exist
        paramiko.SSHException: If SFTP operation fails
        IOError: If file read/write operation fails
        Exception: For other upload errors
    """
    local_path = Path(local_path)
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {local_path}")

    if not local_path.is_file():
        raise ValueError(f"Path is not a file: {local_path}")

    if remote_filename is None:
        remote_filename = local_path.name

    remote_path = f"{remote_dir}/{remote_filename}"

    try:
        sftp.put(str(local_path), remote_path)
        return remote_path
    except paramiko.SSHException as e:
        raise Exception(f"SFTP upload failed: {e}") from e
    except IOError as e:
        raise Exception(f"File I/O error during SFTP upload: {e}") from e
    except Exception as e:
        raise Exception(
            f"Unexpected error during SFTP upload of {local_path.name}: {e}") from e


def fetch_dps_files(sftp, remote_dir, query_period, local_dir, logger=None):
    """
    Fetch DPS files from SFTP server and remove them from server after successful download.

    Args:
        sftp: SFTP client connection
        remote_dir (str): Remote directory path
        query_period (str): Fiscal year/quarter string for filename matching
        local_dir (Path): Local directory to save files
        logger: Optional logger instance (if None, uses logging.getLogger)

    Returns:
        dict: Dictionary with file paths
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Expand ~ if present (defensive - should already be expanded from config)
    local_dir = Path(os.path.expanduser(str(local_dir)))
    local_dir.mkdir(parents=True, exist_ok=True)

    # lists all files in the remote directory
    file_list = sftp.listdir(remote_dir)
    logger.info(f"Found {len(file_list)} files in {remote_dir}")

    # Look for DPS result files
    dps_results_file = None
    dps_counts_file = None

    for filename in file_list:
        # Case-insensitive matching for filename
        filename_lower = filename.lower()
        if f'dfps-missing-person-results_{query_period.lower()}' in filename_lower or 'dfps-missing-person-results' in filename_lower:
            dps_results_file = filename  # Use original filename from server
        elif f'dfps-missing-person-counts_{query_period.lower()}' in filename_lower or 'dfps-missing-person-counts' in filename_lower:
            dps_counts_file = filename  # Use original filename from server

    file_paths = {}

    if dps_results_file:
        local_path = local_dir / dps_results_file
        remote_path = f"{remote_dir}/{dps_results_file}"
        try:
            sftp.get(remote_path, str(local_path))
            file_paths['dps_results'] = local_path
            logger.info(f"Downloaded: {dps_results_file}")
            # Remove file from server after successful download
            try:
                sftp.remove(remote_path)
                logger.info(f"Removed from server: {dps_results_file}")
            except Exception as e:
                logger.warning(
                    f"Failed to remove {dps_results_file} from server: {e}")
        except Exception as e:
            logger.error(f"Failed to download {dps_results_file}: {e}")
            # Don't remove file from server if download failed

    if dps_counts_file:
        local_path = local_dir / dps_counts_file
        remote_path = f"{remote_dir}/{dps_counts_file}"
        try:
            sftp.get(remote_path, str(local_path))
            file_paths['dps_counts'] = local_path
            logger.info(f"Downloaded: {dps_counts_file}")
            # Remove file from server after successful download
            try:
                sftp.remove(remote_path)
                logger.info(f"Removed from server: {dps_counts_file}")
            except Exception as e:
                logger.warning(
                    f"Failed to remove {dps_counts_file} from server: {e}")
        except Exception as e:
            logger.error(f"Failed to download {dps_counts_file}: {e}")
            # Don't remove file from server if download failed

    return file_paths


def _add_period_and_timestamp_to_filename(filepath, query_period=None, timestamp=None):
    """
    Helper function to add query period and timestamp to a filename.

    Args:
        filepath (Path): Path object for the file
        query_period (str, optional): Fiscal year/quarter string (e.g., "FY2025_Q3")
        timestamp (str, optional): Timestamp string (e.g., "20250115_143022"). 
                                   If None, generates current timestamp.

    Returns:
        str: Modified filename with period and timestamp
    """
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    stem = filepath.stem
    suffix = filepath.suffix

    if query_period:
        # Check if query_period is already in the filename
        if query_period not in stem:
            return f"{stem}_{query_period}_{timestamp}{suffix}"
        else:
            # Query period already present, just add timestamp
            return f"{stem}_{timestamp}{suffix}"
    else:
        return f"{stem}_{timestamp}{suffix}"


def archive_file(filepath, archive_type, configs, query_period=None):
    """
    Archive file with timestamp to archive directory by moving it (not copying).

    This prevents file accumulation in the data directory. The original file
    is moved to the archive, so it no longer exists in the original location.

    Args:
        filepath (str or Path): Path to file to archive
        archive_type (str): Type of archive ('from_dps', 'to_dps', 'to_sharept', 'to_cps')
        configs (dict): Configuration dictionary
        query_period (str, optional): Fiscal year/quarter string for filename (e.g., "FY2025_Q3")

    Returns:
        str: Path to archived file

    Raises:
        FileNotFoundError: If source file doesn't exist
    """
    filepath = Path(filepath)  # converts the filepath string to a Path object
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # Get archive directory
    # Access nested structure: configs['archive']['from_dps']
    # expands the tilde (~) in a file path to the user's home directory
    archive_path = os.path.expanduser(str(configs['archive'][archive_type]))
    archive_dir = Path(archive_path)
    # parents=True creates all parent directories if they don't exist
    # exist_ok=True prevents the error if the directory already exists
    # creates the archive directory if it doesn't exist
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Create archived filename using helper function
    # Note: If file was already uploaded, query_period and timestamp may already be in filename
    # The helper function will check and only add what's missing
    archived_name = _add_period_and_timestamp_to_filename(
        filepath, query_period)

    archived_path = archive_dir / archived_name

    # Move file to archive dir
    shutil.move(str(filepath), str(archived_path))

    return str(archived_path)


def archive_all_files_in_directory(data_dir, archive_type, configs, query_period=None, logger=None):
    """
    Archive all files in a data directory to the corresponding archive directory.

    This is useful for archiving all output files from a previous script that
    may be needed by a subsequent script. Files are archived with query period
    and timestamp added to filenames.

    Args:
        data_dir (str or Path): Directory containing files to archive (e.g., 'data/to_dps')
        archive_type (str): Type of archive ('from_dps', 'to_dps', 'to_sharept', 'to_cps')
        configs (dict): Configuration dictionary
        query_period (str, optional): Fiscal year/quarter string for filename (e.g., "FY2025_Q3")
        logger: Optional logger instance for logging

    Returns:
        int: Number of files archived
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Get data directory path from config
    data_path = os.path.expanduser(str(configs['data'][data_dir]))
    data_dir_path = Path(data_path)

    if not data_dir_path.exists():
        logger.warning(f"Data directory does not exist: {data_dir_path}")
        return 0

    if not data_dir_path.is_dir():
        logger.warning(f"Path is not a directory: {data_dir_path}")
        return 0

    # Get all files in directory (not subdirectories)
    files_to_archive = [f for f in data_dir_path.iterdir() if f.is_file()]

    if not files_to_archive:
        logger.info(f"No files to archive in {data_dir_path}")
        return 0

    logger.info(
        f"Archiving {len(files_to_archive)} file(s) from {data_dir_path}...")

    archived_count = 0
    for filepath in files_to_archive:
        try:
            archive_file(filepath, archive_type, configs, query_period)
            archived_count += 1
            if logger:
                logger.info(f"Archived: {filepath.name}")
        except Exception as e:
            if logger:
                logger.error(f"Failed to archive {filepath.name}: {e}")

    logger.info(
        f"Successfully archived {archived_count} of {len(files_to_archive)} file(s)")
    return archived_count


def get_file_paths(query_period, configs, file_type='from_dps'):
    """
    Get file paths based on query period and file type.

    Args:
        query_period (str): Fiscal year/quarter string (e.g., "FY2025_Q3")
        configs (dict): Configuration dictionary
        file_type (str): Type of file ('from_dps', 'to_dps', 'to_sharept', 'to_cps')

    Returns:
        dict: Dictionary with file paths
    """
    # Access nested structure: configs['data']['from_dps']
    # Expand ~ if present (defensive - should already be expanded from config)
    data_path = os.path.expanduser(str(configs['data'][file_type]))
    data_dir = Path(data_path)

    # TODO: query_period may not be needed here
    if file_type == 'from_dps':
        return {
            'dps_results': data_dir / f'Dfps-missing-person-results_{query_period}.txt',
            'dps_counts': data_dir / f'Dfps-missing-person-counts_{query_period}.txt'
        }
    elif file_type == 'to_dps':
        return {
            'cps_sent': data_dir / f'ReferenceData_{query_period}_doublebarrel.xlsx'
        }
    elif file_type == 'to_sharept':
        return {
            'total_events': data_dir / f'DPS_total_runaway_events_{query_period}.xlsx',
            'not_in_impact': data_dir / f'DPS_not_in_IMPACT_{query_period}.xlsx',
            'not_in_dps': data_dir / f'IMPACT_not_in_DPS_{query_period}.xlsx'
        }
    elif file_type == 'to_cps':
        return {
            'total_events': data_dir / f'DPS_total_runaway_events_{query_period}.xlsx',
            'not_in_impact': data_dir / f'DPS_not_in_IMPACT_{query_period}.xlsx',
            'not_in_dps': data_dir / f'IMPACT_not_in_DPS_{query_period}.xlsx'
        }
    else:
        raise ValueError(f"Unknown file_type: {file_type}")


def upload_files_to_network_drive(file_paths, remote_dir="", configs=None, logger=None, query_period=None):
    """
    Upload files to Windows network drive using SMB (non-interactive).
    Adds query period and timestamp to filenames before uploading (similar to archive_file).

    Args:
        file_paths (list): List of local file paths to upload
        remote_dir (str): Remote directory path (e.g., 'CPS/CPS_Research_Evaluation/2 CVS/Runaways/DPS DUA')
        configs (dict): Configuration dictionary (optional, for credential fallback)
        logger: Logger object for logging (optional)
        query_period (str, optional): Fiscal year/quarter string for filename (e.g., "FY2025_Q3")

    Returns:
        bool: True if all files uploaded successfully, False otherwise
    """
    try:
        from smb.SMBConnection import SMBConnection
    except ImportError:
        error_msg = "pysmb library not installed. Install with: pip install pysmb"
        if logger:
            logger.error(error_msg)
        raise ImportError(error_msg)

    # Network drive configuration
    SERVER = configs['windows_share']['server']
    SHARE = configs['windows_share']['share']
    DOMAIN = configs['windows_share']['domain']
    USERNAME = configs['windows_share']['username']
    PASSWORD = configs['windows_share']['password']

    if logger:
        logger.info(f"Connecting to Windows share: //{SERVER}/{SHARE}")

    # Create SMB connection
    conn = None
    try:
        conn = SMBConnection(
            USERNAME,
            PASSWORD,
            "python-client",
            SERVER,
            domain=DOMAIN,
            use_ntlm_v2=True,
            is_direct_tcp=True
        )

        if not conn.connect(SERVER, 445, timeout=30):
            error_msg = f"Failed to connect to Windows share: //{SERVER}/{SHARE}"
            if logger:
                logger.error(error_msg)
            return False

        if logger:
            logger.info(
                f"Connected successfully. Uploading {len(file_paths)} file(s)...")

        success_count = 0
        failed_count = 0
        temp_files = []  # Track temporary files for cleanup

        try:
            # Upload each file
            for local_file_path in file_paths:
                local_path = Path(local_file_path)

                if not local_path.exists():
                    if logger:
                        logger.warning(
                            f"File not found, skipping: {local_file_path}")
                    failed_count += 1
                    continue

                # Create filename with query_period and timestamp using helper function
                upload_filename = _add_period_and_timestamp_to_filename(
                    local_path, query_period)

                # Create temporary copy with modified name
                temp_path = local_path.parent / upload_filename
                try:
                    shutil.copy2(local_path, temp_path)
                    # add the Path object to the list
                    temp_files.append(temp_path)
                except Exception as e:
                    if logger:
                        logger.error(
                            f"Failed to create temporary copy of {local_path.name}: {e}")
                    failed_count += 1
                    continue

                # Build remote path using the modified filename
                if remote_dir:
                    remote_path = f"{remote_dir}/{upload_filename}".replace(
                        "\\", "/")
                else:
                    remote_path = upload_filename

                try:
                    if logger:
                        logger.info(
                            f"Uploading: {upload_filename} -> {remote_path}")

                    with open(temp_path, 'rb') as f:
                        bytes_uploaded = conn.storeFile(SHARE, remote_path, f)

                    if logger:
                        logger.info(
                            f"Uploaded {upload_filename} ({bytes_uploaded:,} bytes)")
                    success_count += 1

                except Exception as e:
                    if logger:
                        logger.error(
                            f"Failed to upload {upload_filename}: {e}")
                    failed_count += 1
                finally:
                    # Clean up temporary file after upload attempt
                    try:
                        if temp_path.exists():
                            # delete the temporary file similar to os.remove
                            temp_path.unlink()
                            if temp_path in temp_files:
                                # remove the Path object from the list
                                temp_files.remove(temp_path)
                    except Exception as e:
                        if logger:
                            logger.warning(
                                f"Failed to delete temporary file {temp_path}: {e}")
        finally:
            # Ensure all temporary files are cleaned up even if there's an error
            for temp_file in temp_files:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                except Exception as e:
                    if logger:
                        logger.warning(
                            f"Failed to delete temporary file {temp_file}: {e}")

        if logger:
            logger.info(
                f"Upload summary: {success_count} succeeded, {failed_count} failed")

        # return True if all files uploaded successfully, False otherwise
        return failed_count == 0

    finally:
        if conn:
            conn.close()
            if logger:
                logger.info("Disconnected from Windows share")


def get_fiscal_year(date_obj, fiscal_start_month=9):
    """
    Calculate fiscal year from calendar date.

    Fiscal year starts in September (month 9). If the date is in September or later,
    the fiscal year is the next calendar year. Otherwise, it's the current calendar year.

    Args:
        date_obj (date): Date to calculate fiscal year for
        fiscal_start_month (int): Month when fiscal year starts (default: 9 for September)

    Returns:
        int: Fiscal year
    """
    calendar_year = date_obj.year
    calendar_month = date_obj.month

    if calendar_month >= fiscal_start_month:
        return calendar_year + 1
    else:
        return calendar_year


def get_query_period(run_date):
    """
    Determine fiscal year and quarter based on run date.

    Logic:
    - Dec, Jan, Feb -> Q1 (Sep, Oct, Nov of current FY)
    - Mar, Apr, May -> Q2 (Dec, Jan, Feb of current FY)
    - Jun, Jul, Aug -> Q3 (Mar, Apr, May of current FY)
    - Sep (on/after 20th) -> Full FY (previous fiscal year)
    - Sep (before 20th), Oct, Nov -> Q4 (Jun, Jul, Aug of previous FY)

    Args:
        run_date (date): Date when script is run

    Returns:
        tuple: (fiscal_year (int), quarter (int or None), query_period (str))
        query_period format: "FY2025_Q1" or "FY2025" for full year
    """
    fiscal_start_month = 9
    current_fiscal_year = get_fiscal_year(run_date, fiscal_start_month)
    run_month = run_date.month
    run_day = run_date.day

    # Map months to quarters: Dec/Jan/Feb -> Q1, Mar/Apr/May -> Q2, Jun/Jul/Aug -> Q3, Sep/Oct/Nov -> Q4
    quarter_map = {12: 1, 1: 1, 2: 1, 3: 2, 4: 2,
                   5: 2, 6: 3, 7: 3, 8: 3, 9: 4, 10: 4, 11: 4}

    # Special case: September 20+ reports full previous fiscal year
    if run_month == 9 and run_day >= 20:
        fiscal_year = current_fiscal_year - 1
        query_period = f"FY{fiscal_year}"
        return fiscal_year, None, query_period
    else:
        # Determine quarter and fiscal year based on run month
        quarter = quarter_map[run_month]
        fiscal_year = current_fiscal_year - \
            1 if run_month in [9, 10, 11] else current_fiscal_year
        query_period = f"FY{fiscal_year}_Q{quarter}"
        return fiscal_year, quarter, query_period


def get_fiscal_year_dates(fiscal_year):
    """
    Get start and end dates for a fiscal year.

    Fiscal year starts on 09/01 of the previous calendar year and ends on 08/31 of the current calendar year.
    For example, FY2025 starts on 09/01/2024 and ends on 08/31/2025.

    Args:
        fiscal_year (int): Fiscal year (e.g., 2025)

    Returns:
        tuple: (start_date, end_date) as date objects
    """
    calendar_year = fiscal_year - 1  # Fiscal year 2025 starts Sep 2024
    start_date = date(calendar_year, 9, 1)
    end_date = date(fiscal_year, 8, 31)
    return start_date, end_date


def get_quarter_dates(fiscal_year, quarter):
    """
    Get start and end dates for a fiscal quarter.

    Args:
        fiscal_year (int): Fiscal year
        quarter (int): Quarter number (1-4)

    Returns:
        tuple: (start_date, end_date) as date objects
    """
    # Fiscal year starts in September
    # Q1: Sep, Oct, Nov (previous calendar year)
    # Q2: Dec, Jan, Feb
    # Q3: Mar, Apr, May
    # Q4: Jun, Jul, Aug
    # In Quarter 4, data from the previous fiscal year is included.

    calendar_year = fiscal_year - 1  # Fiscal year 2025 starts Sep 2024

    if quarter == 1:
        start_date = date(calendar_year, 9, 1)
        end_date = date(calendar_year, 11, 30)
    elif quarter == 2:
        start_date = date(calendar_year, 12, 1)
        # Handle leap years separately if needed
        end_date = date(fiscal_year, 2, 28)
    elif quarter == 3:
        start_date = date(fiscal_year, 3, 1)
        end_date = date(fiscal_year, 5, 31)
    elif quarter == 4:
        # start_date = date(fiscal_year, 6, 1)
        # end_date = date(fiscal_year, 8, 31)
        start_date, end_date = get_fiscal_year_dates(fiscal_year)
    else:
        raise ValueError(f"Invalid quarter: {quarter}")

    return start_date, end_date


def load_dps_to_database(dps_file, configs, envconfig, logger=None):
    """
    Load DPS file into database using SQL*Loader.

    SQL*Loader skips the header row (SKIP=1) and trailing nullcols handles
    the extra trailing column in data rows, so no file pre-processing is needed.

    Args:
        dps_file (Path): Path to DPS colon-separated file
        configs (dict): Configuration dictionary (must have 'dps_load' section)
        envconfig (dict): Environment configuration
        logger: Optional logger instance

    Raises:
        Exception: If SQL*Loader fails
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    dps_file = Path(dps_file)

    if not dps_file.exists():
        raise FileNotFoundError(f"DPS file not found: {dps_file}")

    # Get SQL*Loader configuration
    dps_load_config = configs.get('dps_load', {})
    if not dps_load_config:
        raise ValueError("Missing 'dps_load' configuration in configs")

    control_file = dps_load_config.get('control')
    if not control_file:
        raise ValueError("Missing 'control' in dps_load configuration")

    # Resolve control file path (relative to script directory)
    script_dir = Path(__file__).parent
    control_path = script_dir / control_file

    if not control_path.exists():
        raise FileNotFoundError(f"Control file not found: {control_path}")

    # Get database credentials
    db = envconfig['db']
    user = envconfig['dbuser']
    pwd = envconfig.get('password')
    if not pwd:
        # Fallback to get_password if password not in envconfig
        pwd = get_password(user, db)

    # Build SQL*Loader command
    cmd = f'sqlldr {user}/{pwd}@{db} control={control_path} data={dps_file} errors=100'

    logger.info(f"Loading DPS data into database using SQL*Loader...")
    logger.info(f"Control file: {control_path}")
    logger.info(f"Data file: {dps_file}")

    try:
        output = check_output(cmd, shell=True).decode()
        logger.info(f"SQL*Loader completed successfully")
        logger.debug(f"SQL*Loader output: {output}")
    except CalledProcessError as e:
        error_msg = f"SQL*Loader failed for DPS file {dps_file} with exit code: {e.returncode}\n"
        if e.output:
            error_msg += e.output.decode()
        logger.error(error_msg)
        raise Exception(f"Failed to load DPS data: {error_msg}")
