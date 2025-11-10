"""
Receive and process data from DPS (Department of Public Safety) workflow.

This script processes missing person data returned from DPS, compares it with
CPS/IMPACT data, and generates three output Excel files:
1. DPS_total_runaway_events_FY_QTR.xlsx - All validated DPS runaway events
2. DPS_not_in_IMPACT_FY_QTR.xlsx - DPS cases not found in IMPACT system
3. IMPACT_not_in_DPS_FY_QTR.xlsx - IMPACT cases actively missing but not in DPS

The script:
- Fetches files from DPS via SFTP
- Queries database for lookup data (sa_98, county-region, person/county data)
- Joins DPS and CPS data by name and date of birth
- Filters for valid cases (in care, within date range, under 18)
- Handles status codes and deduplication
- Generates comparison reports
- Uploads files to SharePoint (if configured)
- Emails notification to Richard Ortega
- Archives input files

Author: Hanlong Fu
Date: 2025-11-08
"""

import pandas as pd
from datetime import datetime, date
from pathlib import Path
import argparse
import traceback
import io

from util import (
    read_config, get_dbconnection, get_query_period,
    get_logger, email_error, get_sftp_connection, list_files_sftp,
    archive_file, get_file_paths, get_quarter_dates,
    get_fiscal_year_dates, fetch_dps_files
)

# Global variables
configs = None
envconfig = None
log = None

# TODO: To be tested
def query_database_data(connection, fy_start_dt, fy_end_dt):
    """
    Query database for required lookup data.
    
    Args:
        connection: Database connection object
        fy_start_dt (date): Fiscal year start date (e.g., date(2024, 9, 1))
        fy_end_dt (date): Fiscal year end date (e.g., date(2025, 8, 31))
        
    Returns:
        tuple: (sa_98 DataFrame, county_lookup DataFrame, toad_data DataFrame)
    """
    # 1. Get sa_98 data from database
    # Format dates as strings in Oracle date format (DD-MON-YYYY)
    fy_start_str = fy_start_dt.strftime('%d-%b-%Y').upper()
    fy_end_str = fy_end_dt.strftime('%d-%b-%Y').upper()
    sa_98_query = f"select * from caps.qtr_dps_cvs_pkg.get_sa_98(to_date('{fy_start_str}', 'DD-MON-YYYY'), to_date('{fy_end_str}', 'DD-MON-YYYY'))"
    sa_98 = pd.read_sql(sa_98_query, connection)
    log.info(f"Retrieved {len(sa_98)} rows from sa_98 (FY: {fy_start_dt} to {fy_end_dt})")
    
    # 2. Get county-region lookup from database
    cnty_lookup_query = 'select cnty_name, sub_reg "region" from caps.cnty_reg_tableau'
    cnty_lookup = pd.read_sql(cnty_lookup_query, connection)
    log.info(f"Retrieved {len(cnty_lookup)} rows from county lookup")
    
    # Get maximum time_load value from sa_sum table
    tl_query = "select max(id_time_load) id_time_load from caps.sa_sum@ware"
    timeload_result = pd.read_sql(tl_query, connection)
    timeload_value = int(timeload_result['ID_TIME_LOAD'].iloc[0])
    log.info(f"Retrieved timeload value from sa_sum table: {timeload_value}")
    
    # 3. Get TOAD_DATA (person/county data) from database
    total_data_query = f"""
      SELECT DISTINCT a.ID_SA_PERSON, a.CD_LEGAL_CNTY, b.DECODE, a.NM_PERSON_NAME 
      FROM caps.SA_SUM@WARE a
      left join caps.CCOUNT@rpt b 
      ON a.CD_LEGAL_CNTY = b.code 
      WHERE ID_TIME_LOAD = {timeload_value}
    """
    toad_data = pd.read_sql(total_data_query, connection)
    log.info(f"Retrieved {len(toad_data)} rows from TOAD_DATA")
    
    return sa_98, cnty_lookup, toad_data

# Tested and working
def load_and_prepare_data(dps_file, cps_file):
    """
    Load and prepare DPS and CPS data files.
    
    Args:
        dps_file (Path): Path to DPS colon-separated text file
        cps_file (Path): Path to CPS Excel file
        
    Returns:
        tuple: (DPS DataFrame, CPS DataFrame)
    """
    # 1. Read DPS returned data (colon-separated text file)
    # the header row does not have trailing colon but data rows do
    log.info(f"Reading DPS data from: {dps_file}")

    with open(dps_file, 'r') as f:
      lines = f.readlines()

    # add trailing colon to header row
    lines[0] = lines[0].rstrip('\n') + ':\n'

    # parse the correct data
    dps_data = pd.read_csv(io.StringIO(''.join(lines)), sep=':', header=0, 
                          dtype=str, keep_default_na=False)

    # remove the last column which is empty
    dps_data = dps_data.iloc[:,:-1] 

    # trim the column names (remove extra spaces)
    dps_data.columns = dps_data.columns.str.strip()

    # trim the data in each column (remove extra spaces)
    dps_data = dps_data.apply(lambda x:x.str.strip())

    log.info(f"Loaded {len(dps_data)} rows from DPS file")
    
    # 2. Read CPS reference data (Excel file sent to DPS)
    log.info(f"Reading CPS data from: {cps_file}")
    cps_data = pd.read_excel(cps_file)
    log.info(f"Loaded {len(cps_data)} rows from CPS file")
    
    return dps_data, cps_data

# Tested and working
def transform_data(dps_data, cps_data):
    """
    Transform and clean DPS and CPS data.
    
    Args:
        dps_data (DataFrame): Raw DPS data
        cps_data (DataFrame): Raw CPS data
        
    Returns:
        tuple: (Transformed DPS DataFrame, Transformed CPS DataFrame)
    """
    # Format CPS date columns
    cps_data = cps_data.copy()
    cps_data['Entered_Care'] = pd.to_datetime(cps_data['Entered_Care'], errors='coerce').dt.date
    cps_data['Exited_Care'] = pd.to_datetime(cps_data['Exited_Care'], errors='coerce')
    cps_data['Exited_Care'] = cps_data['Exited_Care'].fillna(pd.Timestamp('2200-01-01')).dt.date
    cps_data['Date_of_Birth'] = pd.to_datetime(cps_data['Date_of_Birth'], errors='coerce').dt.date
    
    # Rename DPS columns to remove spaces and standardize names
    dps_data = dps_data.copy()
    rename_map = {
        'CPS NAME': 'CPS_NAME',
        'FULL NAME': 'FULL_NAME',
        'CPS DOB': 'CPS_DOB',
        'LAST CONT': 'LAST_CONT',
        'ORI DESC': 'ORI_DESC',
        'COUNTY NAME': 'COUNTY_NAME',
        'ORI PHONE': 'ORI_PHONE',
        'NIC #': 'NIC',
        'LOCATE DTE': 'LOCATE_DT',
        'CLR/CAN DTE': 'CLR_CAN_DT'
    }
    dps_data = dps_data.rename(columns=rename_map)
    
    # Format DPS date columns and clean CPS_NAME (remove spaces for matching)
    date_cols = ['CPS_DOB', 'LAST_CONT', 'LOCATE_DT', 'CLR_CAN_DT']
    for col in date_cols:
        if col in dps_data.columns:
            dps_data[col] = pd.to_datetime(dps_data[col], errors='coerce').dt.date
    
    if 'CPS_NAME' in dps_data.columns:
        dps_data['CPS_NAME'] = dps_data['CPS_NAME'].astype(str).str.replace(' ', '')
    
    # Remove rows with missing critical fields
    dps_data = dps_data[
        dps_data['CPS_DOB'].notna() | dps_data['FULL_NAME'].notna()
    ]
    cps_data = cps_data[
        cps_data['Date_of_Birth'].notna() | cps_data['Name'].notna()
    ]
    
    log.info(f"After filtering: {len(dps_data)} DPS rows, {len(cps_data)} CPS rows")
    
    return dps_data, cps_data

# Tested and working
def prepare_cps_for_join(cps_data):
    """
    Prepare CPS data subset for joining with DPS data.
    
    Args:
        cps_data (DataFrame): CPS data
        
    Returns:
        DataFrame: CPS data subset with only needed fields
    """
    # Create subset with only fields needed for matching and episode tracking
    # Note: We keep separate records for different entry/exit dates per Person_ID
    # because we need to determine if a runaway occurred during a specific care episode
    cps_pid = cps_data[['Name', 'Date_of_Birth', 'Person_ID', 'Entered_Care', 'Exited_Care']].copy()
    cps_pid['Name'] = cps_pid['Name'].astype(str)
    cps_pid = cps_pid.drop_duplicates()
    
    return cps_pid

# Tested and working
def join_dps_cps(dps_data, cps_pid):
    """
    Join DPS data with CPS data by name and date of birth.
    
    Args:
        dps_data (DataFrame): DPS data
        cps_pid (DataFrame): CPS data subset
        
    Returns:
        DataFrame: Joined data
    """
    # Left join: match DPS records to CPS records by name and date of birth
    # This can create multiple rows per person due to:
    # - Multiple name variations (from double-barrel name processing)
    # - Multiple care episodes (different entry/exit dates)
    dps_joined = pd.merge(
        dps_data, cps_pid,
        left_on=['CPS_NAME', 'CPS_DOB'],
        right_on=['Name', 'Date_of_Birth'],
        how='left'
    )
    
    # Verify all DPS records matched
    unmatched = dps_joined[dps_joined['Person_ID'].isna()]
    if len(unmatched) > 0:
        log.warning(f"{len(unmatched)} DPS records did not match to CPS data")
    
    return dps_joined

# Tested and working
def filter_valid_cases(dps_joined):
    """
    Filter for valid cases (under 18 at time of last contact).
    
    Args:
        dps_joined (DataFrame): Joined DPS/CPS data
        
    Returns:
        DataFrame: Filtered data
    """
    # Filter out cases where person was over 18 at time of last contact
    # Handle leap year birthdays (Feb 29 -> Mar 1) for age calculation
    dps_joined = dps_joined.copy()
    
    # Convert CPS_DOB to string, replace Feb 29 with Mar 1, convert back
    dob_str = dps_joined['CPS_DOB'].astype(str)
    dob_str = dob_str.str.replace('-02-29', '-03-01', regex=False)
    dps_joined['CPS_DOB_adj'] = pd.to_datetime(dob_str).dt.date
    
    # Calculate 18th birthday (add 18 years)
    def add_years(d, years):
        try:
            return d.replace(year=d.year + years)
        except ValueError:
            # Handle leap year edge case
            return d.replace(year=d.year + years, day=28)
    
    dps_joined['DT_turn_18'] = dps_joined['CPS_DOB_adj'].apply(
        lambda x: add_years(x, 18) if pd.notna(x) else None
    )
    
    # Filter: must be under 18 at time of last contact
    dps_joined = dps_joined[
        (dps_joined['DT_turn_18'].isna()) | 
        (dps_joined['DT_turn_18'] > dps_joined['LAST_CONT'])
    ]
    
    log.info(f"After age filter: {len(dps_joined)} rows")
    
    return dps_joined

# Tested and working
def handle_status_and_deduplicate(dps_joined, start_date, end_date):
    """
    Handle status codes and deduplicate records.
    
    Args:
        dps_joined (DataFrame): Joined and filtered data
        start_date (date): Quarter start date
        end_date (date): Quarter end date
        
    Returns:
        DataFrame: Deduplicated data
    """
    # Recode status (STS) with numeric prefixes for sorting priority:
    # 1LOC = Located (highest priority - person was found)
    # 2ACTV = Active (person still missing)
    # 3CLRD = Cleared
    # 4CANC = Cancelled (lowest priority)
    status_map = {
        'ACTV': '2ACTV',
        'CANC': '4CANC',
        'CLRD': '3CLRD',
        'LOC': '1LOC'
    }
    
    if 'STS' in dps_joined.columns:
        dps_joined['STS'] = dps_joined['STS'].map(status_map).fillna(dps_joined['STS'])
    
    # Deduplication strategy:
    # 1. For each Person_ID + LAST_CONT combination, keep the record with highest priority status
    # 2. For each Person_ID, keep the record with the most recent LAST_CONT date
    if 'STS' in dps_joined.columns:
        # Sort by STS (priority: 1LOC < 2ACTV < 3CLRD < 4CANC)
        dps_total_2 = dps_joined.sort_values(['Person_ID', 'LAST_CONT', 'STS'])
        dps_total_2 = dps_total_2.groupby(['Person_ID', 'LAST_CONT'], as_index=False).first()
    else:
        dps_total_2 = dps_joined.groupby(['Person_ID', 'LAST_CONT'], as_index=False).first()
    
    # For each Person_ID, keep the most recent LAST_CONT
    dps_total_2 = dps_total_2.sort_values(['Person_ID', 'LAST_CONT'], ascending=[True, False])
    dps_total_2 = dps_total_2.groupby('Person_ID', as_index=False).first()
    
    # Filter for cases that:
    # - Occurred during the quarter (or started before but ended during it)
    # - Occurred while the child was in care (between Entered_Care and Exited_Care)
    mask = (
        (dps_total_2['LAST_CONT'] >= start_date)
        | ((dps_total_2['LAST_CONT'] <= start_date) & 
           dps_total_2['LOCATE_DT'].isna() & 
           dps_total_2['CLR_CAN_DT'].isna())
        | ((dps_total_2['LAST_CONT'] <= start_date) & 
           ((dps_total_2['LOCATE_DT'] >= start_date) | 
            (dps_total_2['CLR_CAN_DT'] >= start_date)))
    ) & (
        (dps_total_2['LAST_CONT'] <= end_date) &
        (dps_total_2['LAST_CONT'] <= dps_total_2['Exited_Care']) &
        (dps_total_2['LAST_CONT'] >= dps_total_2['Entered_Care'])
    )
    
    dps_total = dps_total_2[mask].copy()
    
    # Clean up columns
    cols_to_drop = ['FULL_NAME', 'CPS_NAME', 'DOE', 'ORI', 'DT_turn_18', 'CPS_DOB_adj']
    cols_to_drop = [col for col in cols_to_drop if col in dps_total.columns]
    dps_total = dps_total.drop(columns=cols_to_drop)
    dps_total = dps_total.drop_duplicates()
    
    # Create combined date field for locate/clear dates (used for final deduplication)
    dps_total['CombDate'] = dps_total['LOCATE_DT'].fillna(dps_total['CLR_CAN_DT'])
    
    # Final deduplication: for Person_ID + LAST_CONT combinations with multiple statuses,
    # keep the one with highest priority status, then earliest CombDate
    dps_total = dps_total.sort_values(['Person_ID', 'LAST_CONT', 'STS', 'CombDate'])
    dps_total = dps_total.groupby(['Person_ID', 'LAST_CONT'], as_index=False).first()
    
    log.info(f"Final DPS total: {len(dps_total)} rows")
    
    return dps_total

# TODO: partially tested and need to test resolving paths from config file
def generate_output_files(dps_total, sa_98, toad_data, cnty_lookup, query_period, configs):
    """
    Generate three output Excel files.
    
    Args:
        dps_total (DataFrame): Final DPS data
        sa_98 (DataFrame): SA_98 IMPACT data
        toad_data (DataFrame): TOAD person/county data
        cnty_lookup (DataFrame): County-region lookup
        query_period (str): Fiscal year/quarter string
        configs (dict): Configuration dictionary
        
    Returns:
        dict: Dictionary with output file paths
    """
    file_paths = get_file_paths(query_period, configs, 'to_sharept')
    
    # Ensure output directory exists
    output_dir = file_paths['total_events'].parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Output 1: All DPS runaway events
    log.info(f"Writing total runaway events to: {file_paths['total_events']}")
    dps_total.to_excel(file_paths['total_events'], index=False)
    
    # Output 2: DPS cases not found in IMPACT (sa_98 data)
    not_in_impact = dps_total[~dps_total['Person_ID'].isin(sa_98['CHILD_PID'])].copy()
    
    # Add county and region information
    toad_subset = toad_data[[toad_data.columns[0], toad_data.columns[2], toad_data.columns[3]]].copy()
    toad_subset.columns = ['Person_ID', 'Legal_County', 'Name']
    toad_subset['Person_ID'] = toad_subset['Person_ID'].astype(str)
    
    not_in_impact['Person_ID'] = not_in_impact['Person_ID'].astype(str)
    not_in_impact = pd.merge(not_in_impact, toad_subset, on='Person_ID', how='left')

    # Add legal region and legal county information 
    cnty_lookup.columns = ['Legal_County', 'Legal_Region']
    not_in_impact = pd.merge(not_in_impact, cnty_lookup, on='Legal_County', how='left')
    not_in_impact['Outcome'] = ''  # Blank column for manual review
    
    log.info(f"Writing not_in_IMPACT to: {file_paths['not_in_impact']}")
    not_in_impact.to_excel(file_paths['not_in_impact'], index=False)
    
    # Output 3: IMPACT cases not found in DPS active status
    # Identify actively missing cases in IMPACT (not recovered, legal status TMC/PMC or blank)
    sa_98_active = sa_98[
        sa_98['DT_RECOVERED'].isna() &
        (
            sa_98['LEGAL_STATUS'].isin(['TMC', 'PMC/ Rts Not Term', 'PMC/ Rts Term (All)', 
                                        'PMC/ Rts Term (Mother)', 'PMC/Rts Term (Father)']) |
            sa_98['LEGAL_STATUS'].isna()
        )
    ].copy()
    
    # Identify actively missing cases in DPS (status = 2ACTV)
    dps_active = dps_total[dps_total['STS'] == '2ACTV'].copy()
    
    # Find IMPACT cases that are actively missing but not in DPS active list
    not_in_dps = sa_98_active[
        ~sa_98_active['CHILD_PID'].isin(dps_active['Person_ID'])
    ].copy()
    
    # Remove unnecessary columns (columns 17-28, 0-indexed)
    if len(not_in_dps.columns) > 28:
        cols_to_keep = list(not_in_dps.columns[:17]) + list(not_in_dps.columns[28:])
        not_in_dps = not_in_dps[cols_to_keep]
    
    not_in_dps['Outcome'] = ''  # Blank column for manual review
    
    log.info(f"Writing not_in_DPS to: {file_paths['not_in_dps']}")
    not_in_dps.to_excel(file_paths['not_in_dps'], index=False)
    
    return file_paths


def upload_files_to_sharepoint(file_paths, sharepoint_config, query_period, logger=None):
    """
    Upload files to SharePoint document library.
    
    This function uses Office365-REST-Python-Client to upload files to SharePoint.
    Requires: pip install Office365-REST-Python-Client
    
    Args:
        file_paths (list): List of file paths to upload
        sharepoint_config (dict): SharePoint configuration containing:
            - site_url: SharePoint site URL (e.g., "https://yourorg.sharepoint.com/sites/sitename")
            - username: Username for authentication
            - password: Password for authentication
            - folder_path: Target folder path in SharePoint (e.g., "/Shared Documents/DPS Reports")
        query_period (str): Query period string (e.g., "FY2025_Q3") for logging
        logger: Logger object for logging (optional)
        
    Returns:
        list: List of uploaded file URLs
    """
    try:
        from office365.sharepoint.client_context import ClientContext
        from office365.runtime.auth.user_credential import UserCredential
    except ImportError:
        error_msg = "Office365-REST-Python-Client not installed. Install with: pip install Office365-REST-Python-Client"
        if logger:
            logger.error(error_msg)
        raise ImportError(error_msg)
    
    site_url = sharepoint_config['site_url']
    username = sharepoint_config['username']
    password = sharepoint_config['password']
    folder_path = sharepoint_config.get('folder_path', '/Shared Documents')
    
    if logger:
        logger.info(f"Connecting to SharePoint: {site_url}")
        logger.info(f"Target folder: {folder_path}")
    
    # Initialize SharePoint client context
    ctx = ClientContext(site_url).with_credentials(
        UserCredential(username, password)
    )
    
    # Get target folder
    target_folder = ctx.web.get_folder_by_server_relative_url(folder_path)
    ctx.load(target_folder)
    ctx.execute_query()
    
    uploaded_files = []
    
    # Upload each file
    for file_path in file_paths:
        if not Path(file_path).exists():
            if logger:
                logger.warning(f"File not found, skipping: {file_path}")
            continue
        
        file_name = Path(file_path).name
        
        if logger:
            logger.info(f"Uploading {file_name} to SharePoint...")
        
        # Read file content
        with open(file_path, 'rb') as file:
            file_content = file.read()
        
        # Upload file
        uploaded_file = target_folder.upload_file(file_name, file_content).execute_query()
        
        # Get file URL
        file_url = f"{site_url}{uploaded_file.properties['ServerRelativeUrl']}"
        uploaded_files.append(file_url)
        
        if logger:
            logger.info(f"Successfully uploaded {file_name}")
            logger.info(f"File URL: {file_url}")
    
    if logger:
        logger.info(f"Uploaded {len(uploaded_files)} file(s) to SharePoint")
    
    return uploaded_files


def send_notification_email(query_period, sharepoint_success, uploaded_urls, 
                           sharepoint_config, sharepoint_error, output_email, logger=None):
    """
    Send notification email to Richard Ortega without attachments.
    
    The email content depends on SharePoint upload status:
    - If SharePoint upload succeeded: includes SharePoint links
    - If SharePoint upload failed: includes error message
    - If SharePoint not enabled: includes note about archive location
    
    Args:
        query_period (str): Query period string (e.g., "FY2025_Q3")
        sharepoint_success (bool): Whether SharePoint upload succeeded
        uploaded_urls (list): List of uploaded file URLs (if successful)
        sharepoint_config (dict): SharePoint configuration dictionary (or None)
        sharepoint_error (str): Error message if SharePoint upload failed (or None)
        output_email (str): Recipient email address
        logger: Logger object for logging (optional)
    """
    import smtplib
    from email.message import EmailMessage
    
    # Build subject and body based on SharePoint status
    if sharepoint_success and uploaded_urls:
        # Success case: SharePoint upload succeeded
        subject = f"DPS Comparison Reports - {query_period} (Uploaded to SharePoint)"
        body = f"Please find the DPS comparison reports for {query_period} at the following SharePoint location:\n\n"
        body += f"Folder: {sharepoint_config.get('folder_path', '/Shared Documents')}\n\n"
        body += "Files uploaded:\n"
        for url in uploaded_urls:
            body += f"- {url}\n"
    else:
        # Error case: SharePoint upload failed or not enabled
        if sharepoint_config and sharepoint_config.get('enabled', False):
            subject = f"DPS Comparison Reports - {query_period} (SharePoint Upload Failed)"
            body = f"Warning: The DPS comparison reports for {query_period} could not be uploaded to SharePoint.\n\n"
            body += f"Error: {sharepoint_error or 'Unknown error'}\n\n"
            body += "Please check the logs for more details. The files are available in the archive directory.\n"
        else:
            subject = f"DPS Comparison Reports - {query_period}"
            body = f"The DPS comparison reports for {query_period} have been generated.\n\n"
            body += "Note: SharePoint upload is not enabled. Files are available in the archive directory.\n"
    
    # Send email notification (no attachments)
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = subject
        msg["From"] = "noreply@dfps.texas.gov"
        msg["To"] = output_email
        with smtplib.SMTP("localhost", 25) as server:
            server.send_message(msg)
        if logger:
            logger.info(f"Sent notification email to {output_email} (no attachments)")
    except Exception as e:
        error_msg = f"Failed to send notification email: {e}"
        if logger:
            logger.error(error_msg)
        raise Exception(error_msg)


def main():
    global configs, envconfig, log
    
    parser = argparse.ArgumentParser(description="Receive and process data from DPS")
    parser.add_argument("env", choices=['dev', 'prod', 'qawh'], help="Environment to run")
    args = parser.parse_args()
    
    env = args.env
    configs, envconfig = read_config(env)
    
    # Set up logger
    log_dir = configs['log']['from_dps']
    log = get_logger("receivedps", "receivedps.log", log_dir)
    
    # Add delimiter line for this run
    delimiter = "=" * 80
    log.info(delimiter)
    log.info(f"Starting receivedps.py - Run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(delimiter)
    
    try:
        # Get current date and determine query period (FY2002_Q4)
        run_date = date.today()
        fiscal_year, quarter, query_period = get_query_period(run_date)
        log.info(f"Query period: {query_period}")
        
        # Get fiscal year dates for database query
        fy_start_dt, fy_end_dt = get_fiscal_year_dates(fiscal_year)
        log.info(f"Fiscal year dates: {fy_start_dt} to {fy_end_dt}")
        
        # Get quarter dates for filtering
        if quarter:
            start_date, end_date = get_quarter_dates(fiscal_year, quarter)
        else:
            # Full fiscal year - use fiscal year start/end
            start_date = fy_start_dt
            end_date = fy_end_dt
        
        log.info(f"Date range for filtering: {start_date} to {end_date}")
        
        # Connect to database and query lookup data
        connection = get_dbconnection(envconfig)
        sa_98, cnty_lookup, toad_data = query_database_data(connection, fy_start_dt, fy_end_dt)
        connection.close()
        
        # Get file paths
        # TODO: when fetching from DPS, ask Yukie if the files have fy and qtr info
        file_paths = get_file_paths(query_period, configs, 'from_dps')
        cps_file_paths = get_file_paths(query_period, configs, 'to_dps')
        
        # Fetch files from DPS SFTP (if not already downloaded)
        sftp, ssh = get_sftp_connection(configs)
        remote_dir = configs['sftp']['remote_dir_from_dps']
        
        if not file_paths['dps_results'].exists():
            log.info("Fetching files from DPS SFTP...")
            fetched_files = fetch_dps_files(sftp, remote_dir, query_period, file_paths['dps_results'].parent, logger=log)
            if 'dps_results' in fetched_files:
                file_paths['dps_results'] = fetched_files['dps_results']
        else:
            log.info(f"Using existing DPS file: {file_paths['dps_results']}")
        
        ssh.close()
        
        # Load and prepare data
        dps_data, cps_data = load_and_prepare_data(
            file_paths['dps_results'],
            cps_file_paths['cps_sent']
        )
        
        # Transform data
        dps_data, cps_data = transform_data(dps_data, cps_data)
        
        # Prepare CPS data for joining
        cps_pid = prepare_cps_for_join(cps_data)
        
        # Join DPS and CPS data
        dps_joined = join_dps_cps(dps_data, cps_pid)
        
        # Filter for valid cases
        dps_joined = filter_valid_cases(dps_joined)
        
        # Handle status codes and deduplicate
        dps_total = handle_status_and_deduplicate(dps_joined, start_date, end_date)
        
        # Generate output files
        output_files = generate_output_files(
            dps_total, sa_98, toad_data, cnty_lookup, query_period, configs
        )
        
        # Upload files to SharePoint first (if configured)
        # Only upload not_in_impact and not_in_dps files
        sharepoint_config = configs.get('sharepoint')
        uploaded_urls = []
        sharepoint_success = False
        sharepoint_error = None
        
        if sharepoint_config and sharepoint_config.get('enabled', False):
            # Select only the two files to upload: not_in_impact and not_in_dps
            files_to_upload = [
                output_files['not_in_impact'],
                output_files['not_in_dps']
            ]
            log.info(f"Uploading 2 file(s) to SharePoint (not_in_impact and not_in_dps)...")
            try:
                uploaded_urls = upload_files_to_sharepoint(
                    files_to_upload,
                    sharepoint_config,
                    query_period,
                    logger=log
                )
                log.info(f"Successfully uploaded files to SharePoint. URLs: {uploaded_urls}")
                sharepoint_success = True
            except Exception as e:
                sharepoint_error = str(e)
                log.error(f"Failed to upload to SharePoint: {e}")
                log.error(f"SharePoint upload error details: {sharepoint_error}")
                sharepoint_success = False
        
        # send email notification to Richard Ortega
        output_email = configs['output_email']
        send_notification_email(
            query_period,
            sharepoint_success,
            uploaded_urls,
            sharepoint_config,
            sharepoint_error,
            output_email,
            logger=log
        )
        
        # Archive input files
        log.info("Archiving input files...")
        archive_file(file_paths['dps_results'], 'from_dps', configs, query_period)
        archive_file(cps_file_paths['cps_sent'], 'to_dps', configs, query_period)
        
        # Archive output files
        for filepath in output_files.values():
            archive_file(filepath, 'to_sharept', configs, query_period)
        
        log.info("receivedps.py completed successfully")
        log.info("=" * 80)
        
    except Exception as e:
        log.error(f"Exception: {e}")
        message = f"Error from receivedps.py for {env}\n"
        message += str(e) + "\n"
        message += traceback.format_exc()
        email_error(message, configs.get('error_email', 'brent.jones@dfps.texas.gov'), 
                   "receivedps.py", f"{log_dir}/receivedps.log")
        log.error("=" * 80)
        raise


if __name__ == "__main__":
    main()

