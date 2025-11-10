import pandas as pd
import io
from datetime import datetime, date
from pathlib import Path
import argparse
import traceback
import logging
from logging.handlers import RotatingFileHandler
from util import (get_query_period, get_quarter_dates,
                  get_fiscal_year_dates, get_logger, get_file_paths, read_config)


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

    with open(dps_file, 'r') as f:
        lines = f.readlines()

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

    # 2. Read CPS reference data (Excel file sent to DPS)
    cps_data = pd.read_excel(cps_file)

    return dps_data, cps_data


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
            str).str.replace(' ', '')

    # Remove rows with missing critical fields
    dps_data = dps_data[
        dps_data['CPS_DOB'].notna() | dps_data['FULL_NAME'].notna()
    ]
    cps_data = cps_data[
        cps_data['Date_of_Birth'].notna() | cps_data['Name'].notna()
    ]

    return dps_data, cps_data


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
    cps_pid['Name'] = cps_pid['Name'].astype(str)
    cps_pid = cps_pid.drop_duplicates()

    return cps_pid


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
        print(f"{len(unmatched)} DPS records did not match to CPS data")

    return dps_joined


def filter_valid_cases(dps_joined):
    """
    Filter for valid cases (under 18, in care, within date range).

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

    print(f"After age filter: {len(dps_joined)} rows")

    return dps_joined


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

    # Clean up columns
    cols_to_drop = ['FULL_NAME', 'CPS_NAME',
                    'DOE', 'ORI', 'DT_turn_18', 'CPS_DOB_adj']
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

    print(f"Final DPS total: {len(dps_total)} rows")

    return dps_total


def generate_output_files(dps_total, sa_98, toad_data, cnty_lookup):
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
    # file_paths = get_file_paths(query_period, configs, 'to_sharept')

    # Output 1: All DPS runaway events
    print(f"Writing total runaway events {len(dps_total)} rows")
    dps_total.to_excel('dps_total.xlsx', index=False)

    # Output 2: DPS cases not found in IMPACT (sa_98 data)
    not_in_impact = dps_total[~dps_total['Person_ID'].isin(
        sa_98['CHILD_PID'])].copy()
    print(f'not in impact: {len(not_in_impact)} rows')

    # Add county and region information
    toad_subset = toad_data[[toad_data.columns[0],
                             toad_data.columns[2], toad_data.columns[3]]].copy()
    toad_subset.columns = ['Person_ID', 'Legal_County', 'Name']
    toad_subset['Person_ID'] = toad_subset['Person_ID'].astype(str)

    not_in_impact['Person_ID'] = not_in_impact['Person_ID'].astype(str)
    not_in_impact = pd.merge(not_in_impact, toad_subset,
                             on='Person_ID', how='left')

    # Add legal region and legal county information
    cnty_lookup.columns = ['Legal_County', 'Legal_Region']
    not_in_impact = pd.merge(not_in_impact, cnty_lookup,
                             on='Legal_County', how='left')
    not_in_impact['Outcome'] = ''  # Blank column for manual review

    print(f"Writing not_in_IMPACT: {len(not_in_impact)} rows")
    not_in_impact.to_excel("not_in_impact.xlsx", index=False)

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
        cols_to_keep = list(
            not_in_dps.columns[:17]) + list(not_in_dps.columns[28:])
        not_in_dps = not_in_dps[cols_to_keep]

    not_in_dps['Outcome'] = ''  # Blank column for manual review

    print(f"Writing not_in_DPS: {len(not_in_dps)} rows")
    not_in_dps.to_excel("not_in_dps.xlsx", index=False)


dps_path = "./dfps-missing-person-results.txt"
cps_path = "./ReferenceData_FY2025_Q4_doublebarrel.xlsx"
sa_98_path = "./sa_98.csv"
cnty_path = "./cnty_lookup.csv"
toad_path = "./toad.csv"


dps_dta, cps_dta = load_and_prepare_data(dps_path, cps_path)
sa_98 = pd.read_csv(sa_98_path)
cnty_lookup = pd.read_csv(cnty_path)
toad_data = pd.read_csv(toad_path)

dps_data, cps_data = transform_data(dps_dta, cps_dta)

cps_data = prepare_cps_for_join(cps_data)

dps_cps = join_dps_cps(dps_data, cps_data)

dps_cps = filter_valid_cases(dps_cps)

run_date = date.today()
fiscal_year, quarter, query_period = get_query_period(run_date)
print(
    f'fiscal_year: {fiscal_year}, quarter: {quarter}, query_period: {query_period}')

fy_start_dt, fy_end_dt = get_fiscal_year_dates(fiscal_year)
start_date, end_date = get_quarter_dates(fiscal_year, quarter)
print(
    f'fy_start_dt: {fy_start_dt}, fy_end_dt: {fy_end_dt}, start_date: {start_date}, end_date: {end_date}')

dps_total = handle_status_and_deduplicate(
    dps_cps, date(2024, 9, 1), date(2025, 8, 31))

generate_output_files(dps_total, sa_98, toad_data, cnty_lookup)
