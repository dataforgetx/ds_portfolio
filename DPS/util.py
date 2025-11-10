"""
Utility functions for DPS workflow scripts.

This module contains shared functions for database connections, configuration,
logging, and email notifications that are used by both send2dps.py and receivedps.py.

Author: Hanlong Fu
Date: 2025-11-08
"""

import cx_Oracle
import yaml
from datetime import date, datetime
from pathlib import Path
from subprocess import check_output
import logging
from logging.handlers import RotatingFileHandler
import smtplib
from email.message import EmailMessage
import paramiko
import shutil


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


def read_config(env, config_file='dps_config.yml'):
    """
    Read YAML configuration file for environment-specific settings.
    
    Args:
        env (str): Environment name (dev, prod, qawh)
        config_file (str): Path to config file (default: dps_config.yml)
        
    Returns:
        tuple: (config dict, envconfig dict)
    """
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config['config'], config['env'][env]


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
    pwd = get_password(user, db)
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
    log = logging.getLogger(log_name)
    log.setLevel(logging.INFO)
    
    # Create log directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    log_file_path = f'{log_dir}/{log_file}'
    
    handler = RotatingFileHandler(
        filename=log_file_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5
    )
    
    formatter = logging.Formatter("%(asctime)s - %(levelname)8s - %(message)s")
    handler.setFormatter(formatter)
    
    log.addHandler(handler)
    return log


def email_error(message, error_email, script_name="DPS Script", filename=None):
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
    # Handle both single email string and list of emails
    if isinstance(error_email, list):
        msg["To"] = ", ".join(error_email)
    else:
        msg["To"] = error_email
    
    if filename:
        with open(filename, "rb") as f:
            file_data = f.read()
            msg.add_attachment(file_data, maintype="text", subtype="plain", filename=filename)
    
    try:
        with smtplib.SMTP("localhost", 25) as server:
            server.send_message(msg)
    except Exception as e:
        # If email fails, at least log it
        print(f"Failed to send error email: {e}")


def get_sftp_connection(configs):
    """
    Establish SFTP connection to DPS server.
    
    Args:
        configs (dict): Configuration dictionary with SFTP settings
        
    Returns:
        tuple: (sftp, ssh) - SFTP client and SSH client connections
    """
    sftp_config = configs['sftp']
    hostname = sftp_config['hostname']
    username = sftp_config['username']
    password = sftp_config['password']
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=username, password=password)
    sftp = ssh.open_sftp()
    
    return sftp, ssh  # Return both to keep connection alive


def list_files_sftp(sftp, remote_dir):
    """
    List files in remote SFTP directory.
    
    Args:
        sftp: SFTP client connection
        remote_dir (str): Remote directory path
        
    Returns:
        list: List of filenames
    """
    return sftp.listdir(remote_dir)


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
    """
    local_path = Path(local_path)
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {local_path}")
    
    if remote_filename is None:
        remote_filename = local_path.name
    
    remote_path = f"{remote_dir}/{remote_filename}"
    sftp.put(str(local_path), remote_path)
    
    return remote_path


def fetch_dps_files(sftp, remote_dir, query_period, local_dir, logger=None):
    """
    Fetch DPS files from SFTP server.
    
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
    
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    
    file_list = list_files_sftp(sftp, remote_dir)
    logger.info(f"Found {len(file_list)} files in {remote_dir}")
    
    # Look for DPS result files
    dps_results_file = None
    dps_counts_file = None
    
    for filename in file_list:
        # TODO: query_period may not be needed here
        if f'Dfps-missing-person-results_{query_period}' in filename or 'Dfps-missing-person-results' in filename:
            dps_results_file = filename
        elif f'Dfps-missing-person-counts_{query_period}' in filename or 'Dfps-missing-person-counts' in filename:
            dps_counts_file = filename
    
    file_paths = {}
    
    if dps_results_file:
        local_path = local_dir / dps_results_file
        sftp.get(f"{remote_dir}/{dps_results_file}", str(local_path))
        file_paths['dps_results'] = local_path
        logger.info(f"Downloaded: {dps_results_file}")
    
    if dps_counts_file:
        local_path = local_dir / dps_counts_file
        sftp.get(f"{remote_dir}/{dps_counts_file}", str(local_path))
        file_paths['dps_counts'] = local_path
        logger.info(f"Downloaded: {dps_counts_file}")
    
    return file_paths


def archive_file(filepath, archive_type, configs, query_period=None):
    """
    Archive file with timestamp to archive directory by moving it (not copying).
    
    This prevents file accumulation in the data directory. The original file
    is moved to the archive, so it no longer exists in the original location.
    
    Args:
        filepath (str or Path): Path to file to archive
        archive_type (str): Type of archive ('from_dps', 'to_dps', 'to_sharept')
        configs (dict): Configuration dictionary
        query_period (str, optional): Fiscal year/quarter string for filename (e.g., "FY2025_Q3")
        
    Returns:
        str: Path to archived file
        
    Raises:
        FileNotFoundError: If source file doesn't exist
        OSError: If move operation fails
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    # Create timestamp
    timestamp = datetime.now().strftime('%y%m%d_%H%M%S')
    
    # Get archive directory
    # Access nested structure: configs['archive']['from_dps']
    archive_dir = Path(configs['archive'][archive_type])
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Create archived filename
    if query_period:
        # Insert query_period before extension if not already present
        stem = filepath.stem
        if query_period not in stem:
            archived_name = f"{stem}_{query_period}{filepath.suffix}.{timestamp}"
        else:
            archived_name = f"{stem}{filepath.suffix}.{timestamp}"
    else:
        archived_name = f"{filepath.name}.{timestamp}"
    
    archived_path = archive_dir / archived_name
    
    # Move file to archive (atomic on same filesystem, copy+delete on different filesystem)
    shutil.move(str(filepath), str(archived_path))
    
    return str(archived_path)


def get_file_paths(query_period, configs, file_type='from_dps'):
    """
    Get file paths based on query period and file type.
    
    Args:
        query_period (str): Fiscal year/quarter string (e.g., "FY2025_Q3")
        configs (dict): Configuration dictionary
        file_type (str): Type of file ('from_dps', 'to_dps', 'to_sharept')
        
    Returns:
        dict: Dictionary with file paths
    """
    # Access nested structure: configs['data']['from_dps']
    data_dir = Path(configs['data'][file_type])
    
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
    else:
        raise ValueError(f"Unknown file_type: {file_type}")


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
    quarter_map = {12: 1, 1: 1, 2: 1, 3: 2, 4: 2, 5: 2, 6: 3, 7: 3, 8: 3, 9: 4, 10: 4, 11: 4}
    
    # Special case: September 20+ reports full previous fiscal year
    if run_month == 9 and run_day >= 20:
        fiscal_year = current_fiscal_year - 1
        query_period = f"FY{fiscal_year}"
        return fiscal_year, None, query_period
    else:
        # Determine quarter and fiscal year based on run month
        quarter = quarter_map[run_month]
        fiscal_year = current_fiscal_year - 1 if run_month in [9, 10, 11] else current_fiscal_year
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
        end_date = date(fiscal_year, 2, 28)  # Handle leap years separately if needed
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

