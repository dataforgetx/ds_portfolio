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
- Uploads files to Windows network drive
- Sends email notifications (success/error based on upload status)
- Archives input and output files
"""

import argparse
import io
import traceback
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from util import (
    read_config_from_script_dir, get_dbconnection, get_query_period,
    get_logger, email_error, get_sftp_connection,
    archive_file, get_file_paths, get_quarter_dates,
    get_fiscal_year_dates, fetch_dps_files, upload_files_to_network_drive,
    send_notification_email as send_notification_email_util,
    load_dps_to_database
)

# Global variables
configs = None
envconfig = None
log = None

# TODO: To be tested


def query_database_data(connection, fy_start_dt, fy_end_dt, run_date):
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
    log.info(
        f"Retrieved {len(sa_98)} rows from sa_98 (FY: {fy_start_dt} to {fy_end_dt})")

    # 2. Get county-region lookup from database
    cnty_lookup_query = 'select cnty_name, sub_reg "region" from caps.cnty_reg_tableau'
    cnty_lookup = pd.read_sql(cnty_lookup_query, connection)
    log.info(f"Retrieved {len(cnty_lookup)} rows from county lookup")

    # Get the right time_load value from sa_sum table
    tl_query = f"""
        select distinct id_time_load from caps.time_dim
        where nbr_time_calendar_year = {run_date.year}
        and nbr_time_calendar_month = {run_date.month-1}
    """
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

    # Validate file is not empty
    if not lines:
        raise ValueError(f"DPS file is empty: {dps_file}")

    # add trailing colon to header row
    lines[0] = lines[0].rstrip('\n') + ':\n'

    # parse the correct data
    dps_data = pd.read_csv(io.StringIO(''.join(lines)), sep=':', header=0,
                           dtype=str, keep_default_na=False)

    # remove the last column which is empty
    dps_data = dps_data.iloc[:, :-1]

    # trim the column names (remove extra spaces)
    dps_data.columns = dps_data.columns.str.strip()

    # trim the data in each column (remove extra spaces)
    dps_data = dps_data.apply(lambda x: x.str.strip())

    log.info(f"Loaded {len(dps_data)} rows from DPS file")

    # 2. Read CPS reference data (Excel file sent to DPS)
    log.info(f"Reading CPS data from: {cps_file}")
    # Use openpyxl engine for .xlsx files (xlrd doesn't support .xlsx)
    cps_data = pd.read_excel(cps_file, engine='openpyxl')
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
    cps_data['Entered_Care'] = pd.to_datetime(
        cps_data['Entered_Care'], errors='coerce').dt.date
    cps_data['Exited_Care'] = pd.to_datetime(
        cps_data['Exited_Care'], errors='coerce')
    cps_data['Exited_Care'] = cps_data['Exited_Care'].fillna(
        pd.Timestamp('2200-01-01')).dt.date
    cps_data['Date_of_Birth'] = pd.to_datetime(
        cps_data['Date_of_Birth'], errors='coerce').dt.date

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
            dps_data[col] = pd.to_datetime(
                dps_data[col], errors='coerce').dt.date

    if 'CPS_NAME' in dps_data.columns:
        dps_data['CPS_NAME'] = dps_data['CPS_NAME'].astype(
            str).str.replace(' ', '').str.upper()

    # Remove rows with missing critical fields
    dps_data = dps_data[
        dps_data['CPS_DOB'].notna() | dps_data['FULL_NAME'].notna()
    ]
    cps_data = cps_data[
        cps_data['Date_of_Birth'].notna() | cps_data['Name'].notna()
    ]

    log.info(
        f"After filtering: {len(dps_data)} DPS rows, {len(cps_data)} CPS rows")

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
    cps_pid = cps_data[['Name', 'Date_of_Birth',
                        'Person_ID', 'Entered_Care', 'Exited_Care']].copy()
    # Normalize Name to match DPS format: remove spaces, strip whitespace, uppercase
    # This must match the format used for CPS_NAME in transform_data()
    cps_pid['Name'] = cps_pid['Name'].astype(
        str).str.strip().str.replace(' ', '').str.upper()
    # Filter out rows with NaT dates before join (they won't match anyway)
    cps_pid = cps_pid[cps_pid['Date_of_Birth'].notna()]
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
    # Filter out DPS rows with NaT dates before join (they won't match anyway)
    dps_data_for_join = dps_data[dps_data['CPS_DOB'].notna()].copy()

    dps_joined = pd.merge(
        dps_data_for_join, cps_pid,
        left_on=['CPS_NAME', 'CPS_DOB'],
        right_on=['Name', 'Date_of_Birth'],
        how='left'
    )

    # Re-add DPS rows with NaT dates (they won't match but should be included)
    dps_data_nat = dps_data[dps_data['CPS_DOB'].isna()].copy()
    if len(dps_data_nat) > 0:
        # Add empty columns for join fields
        for col in ['Name', 'Date_of_Birth', 'Person_ID', 'Entered_Care', 'Exited_Care']:
            if col not in dps_data_nat.columns:
                dps_data_nat[col] = None
        dps_joined = pd.concat([dps_joined, dps_data_nat], ignore_index=True)

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
        dps_joined['STS'] = dps_joined['STS'].map(
            status_map).fillna(dps_joined['STS'])

    # Deduplication strategy:
    # 1. For each Person_ID + LAST_CONT combination, keep the record with highest priority status
    # 2. For each Person_ID, keep the record with the most recent LAST_CONT date
    if 'STS' in dps_joined.columns:
        # Sort by STS (priority: 1LOC < 2ACTV < 3CLRD < 4CANC)
        dps_total_2 = dps_joined.sort_values(['Person_ID', 'LAST_CONT', 'STS'])
        dps_total_2 = dps_total_2.groupby(
            ['Person_ID', 'LAST_CONT'], as_index=False).first()
    else:
        dps_total_2 = dps_joined.groupby(
            ['Person_ID', 'LAST_CONT'], as_index=False).first()

    # For each Person_ID, keep the most recent LAST_CONT
    dps_total_2 = dps_total_2.sort_values(
        ['Person_ID', 'LAST_CONT'], ascending=[True, False])
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

    # Clean up columns - remove Name and Date_of_Birth from join (not needed in final output)
    cols_to_drop = ['FULL_NAME', 'CPS_NAME', 'DOE', 'ORI',
                    'DT_turn_18', 'CPS_DOB_adj', 'Name', 'Date_of_Birth']
    cols_to_drop = [col for col in cols_to_drop if col in dps_total.columns]
    dps_total = dps_total.drop(columns=cols_to_drop)
    dps_total = dps_total.drop_duplicates()

    # Create combined date field for locate/clear dates (used for final deduplication)
    dps_total['CombDate'] = dps_total['LOCATE_DT'].fillna(
        dps_total['CLR_CAN_DT'])

    # Final deduplication: for Person_ID + LAST_CONT combinations with multiple statuses,
    # keep the one with highest priority status, then earliest CombDate
    dps_total = dps_total.sort_values(
        ['Person_ID', 'LAST_CONT', 'STS', 'CombDate'])
    dps_total = dps_total.groupby(
        ['Person_ID', 'LAST_CONT'], as_index=False).first()

    log.info(f"Final DPS total: {len(dps_total)} rows")

    return dps_total


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
    file_paths = get_file_paths(query_period, configs, 'to_cps')

    # Ensure output directory exists
    output_dir = file_paths['total_events'].parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Output 1: All DPS runaway events
    expected_cols = ['CPS_DOB', 'S', 'R', 'E', 'LAST_CONT', 'HR', 'ORI_DESC',
                     'COUNTY_NAME', 'ORI_PHONE', 'NIC', 'STS', 'LOCATE_DT', 'CLR_CAN_DT',
                     'Person_ID', 'Entered_Care', 'Exited_Care', 'CombDate']
    # Only include columns that exist in the dataframe
    cols_to_use = [col for col in expected_cols if col in dps_total.columns]
    # Add any remaining columns that aren't in expected list (shouldn't happen, but just in case)
    remaining_cols = [
        col for col in dps_total.columns if col not in expected_cols]
    dps_total_ordered = dps_total[cols_to_use + remaining_cols].copy()

    log.info(f"Writing total runaway events to: {file_paths['total_events']}")
    dps_total_ordered.to_excel(file_paths['total_events'], index=False)

    # Output 2: DPS cases not found in IMPACT (sa_98 data)
    not_in_impact = dps_total[~dps_total['Person_ID'].isin(
        sa_98['CHILD_PID'])].copy()

    # Add county and region information
    toad_subset = toad_data[[toad_data.columns[0],
                             toad_data.columns[2], toad_data.columns[3]]].copy()
    toad_subset.columns = ['Person_ID', 'Legal_County', 'Name']
    toad_subset['Person_ID'] = toad_subset['Person_ID'].astype(str)
    toad_subset['Legal_County'] = toad_subset['Legal_County'].astype(
        str).str.strip()

    not_in_impact['Person_ID'] = not_in_impact['Person_ID'].astype(str)
    # Drop any existing Name column from not_in_impact before merge to avoid Name_x/Name_y
    if 'Name' in not_in_impact.columns:
        not_in_impact = not_in_impact.drop(columns=['Name'])

    not_in_impact = pd.merge(not_in_impact, toad_subset,
                             on='Person_ID', how='left', suffixes=('', '_toad'))

    # Add legal region information
    cnty_lookup.columns = ['Legal_County', 'Legal_Region']
    cnty_lookup['Legal_County'] = cnty_lookup['Legal_County'].astype(
        str).str.strip()
    not_in_impact = pd.merge(not_in_impact, cnty_lookup,
                             on='Legal_County', how='left')
    not_in_impact['Outcome'] = ''  # Blank column for manual review

    # Define column order
    expected_cols = ['CPS_DOB', 'S', 'R', 'E', 'LAST_CONT', 'HR', 'ORI_DESC',
                     'COUNTY_NAME', 'ORI_PHONE', 'NIC', 'STS', 'LOCATE_DT', 'CLR_CAN_DT', 'Person_ID',
                     'Entered_Care', 'Exited_Care', 'CombDate', 'Legal_County', 'Name', 'Legal_Region', 'Outcome']
    # Only include columns that exist in the dataframe
    cols_to_use = [
        col for col in expected_cols if col in not_in_impact.columns]
    # Add any remaining columns that aren't in expected list
    remaining_cols = [
        col for col in not_in_impact.columns if col not in expected_cols]
    not_in_impact_ordered = not_in_impact[cols_to_use + remaining_cols].copy()

    log.info(f"Writing not_in_IMPACT to: {file_paths['not_in_impact']}")
    not_in_impact_ordered.to_excel(file_paths['not_in_impact'], index=False)

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

    # Remove unnecessary columns
    cols_to_remove = [
        'DT_RECOVERED', 'RETURNED_BY', 'RECOVERY_INTERVW_CONDUCTED',
        'RSN_NOT_INTERVIEWED', 'RECOVERY_INTERVW_DT', 'CONFIRMED_RSN_ABSENCE',
        'SEX_TRF', 'LABOR_TRF', 'PHAB', 'SXAB', 'OTHER', 'TXT_RUN_RSN'
    ]
    cols_to_remove = [
        col for col in cols_to_remove if col in not_in_dps.columns]
    not_in_dps = not_in_dps.drop(columns=cols_to_remove)

    not_in_dps['Outcome'] = ''  # Blank column for manual review

    log.info(f"Writing not_in_DPS to: {file_paths['not_in_dps']}")
    not_in_dps.to_excel(file_paths['not_in_dps'], index=False)

    return file_paths


def main():
    global configs, envconfig, log

    parser = argparse.ArgumentParser(
        description="Receive and process data from DPS")
    parser.add_argument(
        "env", choices=['dev', 'prod', 'qawh'], help="Environment to run")
    args = parser.parse_args()

    env = args.env
    # Use dps_config_test.yml exclusively
    configs, envconfig = read_config_from_script_dir(
        env, 'dps_config_test.yml')

    # Set up logger
    log_dir = configs['log']['from_dps']
    log = get_logger("receivedps", "receivedps.log", log_dir)

    # Add delimiter line for this run
    delimiter = "=" * 80
    log.info(delimiter)
    log.info(
        f"Starting receivedps.py - Run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
        connection = None
        try:
            connection = get_dbconnection(envconfig)
            sa_98, cnty_lookup, toad_data = query_database_data(
                connection, fy_start_dt, fy_end_dt)
        finally:
            if connection:
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
            fetched_files = fetch_dps_files(
                sftp, remote_dir, query_period, file_paths['dps_results'].parent, logger=log)
            if 'dps_results' in fetched_files:
                file_paths['dps_results'] = fetched_files['dps_results']
        else:
            log.info(f"Using existing DPS file: {file_paths['dps_results']}")

        # Close SFTP first, then SSH
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

        if file_paths['dps_results'].exists():
            try:
                load_dps_to_database(
                    file_paths['dps_results'], configs, envconfig, logger=log)
            except Exception as e:
                log.warning(f"Failed to load DPS results: {e}")

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
        dps_total = handle_status_and_deduplicate(
            dps_joined, start_date, end_date)

        # Generate output files
        output_files = generate_output_files(
            dps_total, sa_98, toad_data, cnty_lookup, query_period, configs
        )

        # Upload all three output files to network drive
        files_to_upload = [
            output_files['total_events'],
            output_files['not_in_impact'],
            output_files['not_in_dps']
        ]

        remote_directory = configs['windows_share'].get(
            'remote_dir', 'CPS/CPS_Research_Evaluation/2 CVS/Runaways/DPS DUA')

        log.info(
            f"Uploading {len(files_to_upload)} file(s) to network drive...")
        upload_success = False
        upload_error = None
        try:
            upload_success = upload_files_to_network_drive(
                files_to_upload,
                remote_dir=remote_directory,
                configs=configs,
                logger=log,
                query_period=query_period
            )
            if upload_success:
                log.info("Successfully uploaded all files to network drive")
            else:
                log.warning(
                    "Some files may have failed to upload to network drive")
                upload_error = "Some files failed to upload"
        except Exception as e:
            log.error(f"Failed to upload files to network drive: {e}")
            log.error(f"Upload error details: {traceback.format_exc()}")
            upload_error = str(e)

        # Send notification email based on upload status
        try:
            # Get recipient emails from config
            if upload_success:
                recipient_emails = configs.get(
                    'success_email', ['hanlong.fu2@dfps.texas.gov'])
                subject = f"DPS Comparison Reports - {query_period} (Uploaded to Network Drive)"
                server = configs['windows_share']['server']
                share = configs['windows_share']['share']
                remote_dir_formatted = remote_directory.replace('/', '\\')
                network_path = f"\\\\{server}\\{share}\\{remote_dir_formatted}"
                message = f"The DPS comparison reports for {query_period} have been successfully uploaded to the network drive.\n\n"
                message += f"Files are available at: {network_path}\n\n"
                message += "Files uploaded:\n"
                message += "- DPS_total_runaway_events\n"
                message += "- DPS_not_in_IMPACT\n"
                message += "- IMPACT_not_in_DPS\n"
            else:
                recipient_emails = configs.get(
                    'error_email', ['hanlong.fu2@dfps.texas.gov'])
                subject = f"DPS Comparison Reports - {query_period} (Network Drive Upload Failed)"
                message = f"Warning: The DPS comparison reports for {query_period} could not be uploaded to the network drive.\n\n"
                if upload_error:
                    message += f"Error: {upload_error}\n\n"
                message += "Please check the logs for more details. The files are available in the archive directory.\n"

            send_notification_email_util(
                recipient_emails, subject, message, logger=log)
        except Exception as e:
            log.warning(f"Failed to send notification email: {e}")

        # Archive input files
        log.info("Archiving input files...")
        archive_file(file_paths['dps_results'],
                     'from_dps', configs, query_period)

        # Archive output files
        for filepath in output_files.values():
            archive_file(filepath, 'to_cps', configs, query_period)

        # Archive all files from send2dps.py output (data/to_dps)
        # These files are needed by receivedps.py, so we archive them after processing
        log.info("Archiving files from send2dps.py output...")
        archive_all_files_in_directory(
            'to_dps', 'to_dps', configs, query_period, logger=log)

        log.info("receivedps.py completed successfully")
        log.info("=" * 80)

    except Exception as e:
        log.error(f"Exception: {e}")
        log.error(traceback.format_exc())
        message = f"Error from receivedps.py for {env}\n"
        message += f"{str(e)}\n"
        message += traceback.format_exc()
        error_emails = configs.get(
            'error_email', ['hanlong.fu2@dfps.texas.gov'])
        email_error(message, error_emails, "receivedps.py",
                    f"{log_dir}/receivedps.log")
        log.error("=" * 80)
        raise


if __name__ == "__main__":
    main()
