"""
Send data to DPS (Department of Public Safety) workflow.

This script queries Oracle database for children in care data, performs extensive
data wrangling (name processing, deduplication, double-barrel name handling), and
generates two output files:
1. Reference Excel file with all variables for internal use
2. Fixed-width text file formatted for DPS submission

The script determines the fiscal year and quarter based on the run date and filters
data accordingly. It handles complex name transformations including:
- Removing accents from names
- Splitting double-barrel last names (hyphenated and spaced)
- Splitting double first names (hyphenated, apostrophe, and spaced)
- Deduplication logic for multiple episodes in care
- Gender code conversion (Male->M, Female->F, Unknown->U)

Output files are named dynamically based on fiscal year and quarter:
- ReferenceData_FY{year}_Q{quarter}_doublebarrel.xlsx
- Children_in_care_FY{year}_Q{quarter}.txt

Author: Hanlong Fu
Date: 2025-11-08
"""

import pandas as pd
from datetime import datetime, date
import traceback
import argparse
from unidecode import unidecode

from util import (
    read_config, get_dbconnection, get_fiscal_year,
    get_logger, email_error, get_query_period as get_query_period_util,
    get_sftp_connection, upload_file_sftp, archive_file, get_file_paths
)

# Global variables
configs = None
envconfig = None
log = None


def get_filtered_data_for_period(run_date, envconfig):
    """
    Query database and filter data based on fiscal year and quarter.
    
    This function queries the database and filters data based on the query period
    determined by get_query_period from util.py.
    
    Args:
        run_date (date): Date when script is run
        envconfig (dict): Environment configuration dictionary
        
    Returns:
        tuple: (filtered DataFrame, query_period string like "FY2025_Q1")
    """
    # Get fiscal year, quarter, and query_period from util.py
    fiscal_year, quarter, query_period = get_query_period_util(run_date)
    
    # Get data from database
    connection = get_dbconnection(envconfig)
    query = "SELECT * FROM caps.qtr_dps_cvs"
    df = pd.read_sql(query, connection)
    connection.close()

    # Read from Excel file for testing (comment out when querying SQL table)
    # df = pd.read_excel('test.xlsx')
    log.info(f"Total rows retrieved from database: {len(df)}")
    
    # Filter data based on fiscal year and quarter
    if quarter is None:
        # Full fiscal year
        filtered_data = df[df['RPT_FISCAL_YR'] == fiscal_year]
    else:
        # Specific quarter
        filtered_data = df[
            (df['RPT_FISCAL_YR'] == fiscal_year) &
            (df['RPT_QTR'] == quarter)
        ]
    
    log.info(f"Querying data for: {query_period}")
    log.info(f"Filtered rows: {len(filtered_data)}")
    
    return filtered_data, query_period


def filter_blank_values(df, col_names):
    """
    Filter out rows with blank or NA values in the specified column(s).
    
    Args:
        df (DataFrame): Input dataframe
        col_names (str or list): Column name(s) to filter on (single string or list of strings)
        
    Returns:
        DataFrame: Dataframe with blank/NA values removed
    """
    # Handle both single column (string) and multiple columns (list)
    if isinstance(col_names, str):
        col_names = [col_names]
    
    # Create combined mask for all columns
    mask = None
    for col in col_names:
        col_mask = df[col].notna() & (df[col].str.strip() != '')
        mask = col_mask if mask is None else mask & col_mask
    
    return df[mask]


def split_name_by_delimiter(df, col_name, delimiter, max_parts, exclude_list=None, remove_delimiter=True):
    """
    Split names by delimiter and create additional rows for each part.
    
    For example, "SMITH-JONES" with delimiter '-' and max_parts=2 creates:
    - Original row with "SMITH-JONES" (if remove_delimiter=False) or "SMITHJONES" (if True)
    - New row with "SMITH"
    - New row with "JONES"
    
    Args:
        df (DataFrame): Input dataframe
        col_name (str): Column name to split (e.g., 'Last Name', 'First Name')
        delimiter (str): Delimiter to split on (e.g., '-', "'", ' ')
        max_parts (int): Maximum number of parts to split into
        exclude_list (list, optional): List of values to exclude after splitting
        remove_delimiter (bool): Whether to remove delimiter from original column
        
    Returns:
        DataFrame: Dataframe with expanded name variations
    """
    # Filter rows containing the delimiter
    mask = df[col_name].str.contains(delimiter, na=False, regex=False)
    if not mask.any():
        return df
    
    # Get rows with delimiter
    rows_with_delimiter = df[mask].copy()
    
    # Create split parts
    split_parts = []
    for i in range(max_parts):
        part = rows_with_delimiter.copy()
        # Split and get the i-th part
        part[col_name] = part[col_name].str.split(delimiter).str[i]
        split_parts.append(part)
    
    # Combine all split parts
    split_combined = pd.concat(split_parts, ignore_index=True)
    
    # Filter out blank values
    split_combined = filter_blank_values(split_combined, col_name)
    
    # Apply exclude list if provided
    if exclude_list:
        split_combined = split_combined[~split_combined[col_name].isin(exclude_list)]
    
    # Combine with original dataframe
    df = pd.concat([df, split_combined], ignore_index=True)
    
    # Remove delimiter from original column if requested
    if remove_delimiter:
        df[col_name] = df[col_name].str.replace(delimiter, '', regex=False)
    
    return df


def process_names(df):
    """
    Process and clean name fields, handling accents, double-barrel names, etc.
    
    This function performs extensive name processing:
    1. Removes accents from names
    2. Handles double-barrel last names (hyphens and spaces)
    3. Handles double first names (hyphens, apostrophes, and spaces)
    4. Removes non-alphanumeric characters
    5. Converts to uppercase
    
    Args:
        df (DataFrame): Input dataframe with name columns
        
    Returns:
        DataFrame: Processed dataframe with expanded name variations
    """
    # Rename columns to match R script expectations
    df = df.rename(columns={
        'ID_PP_PERSON': 'Person ID',
        'NM_PERSON_FIRST': 'First Name',
        'NM_PERSON_MIDDLE': 'Middle Name',
        'NM_PERSON_LAST': 'Last Name',
        'DT_CHILD_BIRTH': 'Date of Birth',
        'DT_ENTER_CARE': 'Entered Care',
        'DT_EXIT_CARE': 'Exited Care',
        'GENDER': 'Gender'
    })
    
    # Filter out rows with missing critical name fields early
    df = filter_blank_values(df, ['Last Name', 'First Name'])
    
    # Convert Person ID to string and remove commas
    df['Person ID'] = df['Person ID'].astype(str).str.replace(',', '')
    
    # Remove accents from names
    for col in ['First Name', 'Middle Name', 'Last Name']:
        df[col] = df[col].apply(lambda x: unidecode(str(x)) if pd.notna(x) else x)
    
    # Create concatenated field for deduplication
    df['concat'] = df['Person ID'].astype(str) + '_' + df['Entered Care'].astype(str)
    
    # Sort by concat and exit date, then take first row for each concat
    df = df.sort_values(['concat', 'Exited Care'], na_position='last')
    df = df.drop_duplicates(subset=['concat'], keep='first')
    
    # Make all names uppercase (handle NaN first)
    for col in ['Last Name', 'Middle Name', 'First Name']:
        df[col] = df[col].fillna('').astype(str).str.upper()
    
    # Process double-barrel last names
    df = process_double_barrel_last_names(df)
    
    # Process double first names
    df = process_double_first_names(df)
    
    # Remove exact duplicates (consolidated - once at end after all processing)
    df = df.drop_duplicates()
    
    return df


def process_double_barrel_last_names(df):
    """
    Handle double-barrel last names by creating rows for each part.
    
    Creates additional rows for:
    1. Hyphenated names (e.g., "SMITH-JONES" -> "SMITH", "JONES", "SMITHJONES")
    2. Spaced names (e.g., "DE LA CRUZ" -> "DE", "LA", "CRUZ", "DELACRUZ")
    
    Args:
        df (DataFrame): Input dataframe
        
    Returns:
        DataFrame: Dataframe with expanded last name variations
    """
    # 1) Hyphenated last names
    df = split_name_by_delimiter(df, 'Last Name', '-', max_parts=2, remove_delimiter=True)
    
    # 2) Last names with spaces
    exclude_list = ['LA', 'MC', 'DE', 'ST', 'ST.', 'DEL', 'JR', 'JR.', 'A', 'O']
    df = split_name_by_delimiter(df, 'Last Name', ' ', max_parts=5, exclude_list=exclude_list, remove_delimiter=False)
    
    # Remove spaces from last name
    df['Last Name'] = df['Last Name'].str.replace(' ', '', regex=False)
    
    # Remove non-alphanumeric characters (but keep spaces for now, we'll remove them)
    df['Last Name'] = df['Last Name'].str.replace(r'[^A-Za-z0-9 ]', '', regex=True)
    
    # Remove blank last names
    df = filter_blank_values(df, ['Last Name'])
    
    return df


def process_double_first_names(df):
    """
    Handle double first names by creating rows for each part.
    
    Creates additional rows for:
    1. Hyphenated names (e.g., "MARY-JANE" -> "MARY", "JANE")
    2. Apostrophe names (e.g., "O'CONNOR" -> "O", "CONNOR")
    3. Spaced names (e.g., "MARY JANE" -> "MARY", "JANE")
    
    Args:
        df (DataFrame): Input dataframe
        
    Returns:
        DataFrame: Dataframe with expanded first name variations
    """
    # 1) Hyphenated or apostrophe first names
    df = split_name_by_delimiter(df, 'First Name', '-', max_parts=2, remove_delimiter=True)
    df = split_name_by_delimiter(df, 'First Name', "'", max_parts=2, remove_delimiter=True)
    
    # Remove non-alphanumeric characters from middle names (First Name delimiters already removed)
    df['Middle Name'] = df['Middle Name'].str.replace(r'[^A-Za-z0-9 ]', '', regex=True)
    
    # 2) First names with spaces
    df = split_name_by_delimiter(df, 'First Name', ' ', max_parts=3, remove_delimiter=True)
    
    # Remove blank first names
    df = filter_blank_values(df, ['First Name'])
    
    # Remove spaces from middle names (First Name spaces already removed by split_name_by_delimiter)
    df['Middle Name'] = df['Middle Name'].str.replace(' ', '', regex=False)
    
    # Remove first names with 2 or fewer characters
    df = df[df['First Name'].str.len() > 2]
    
    # Final comprehensive filter - remove any rows with missing or invalid names
    # This catches any edge cases from string operations that might create "nan" strings
    df = filter_blank_values(df, ['Last Name', 'First Name'])
    for col in ['Last Name', 'First Name']:
        df = df[df[col].str.upper() != 'NAN']
    
    return df


def create_final_dataset(df):
    """
    Create final dataset with proper column names and formatting.
    
    Args:
        df (DataFrame): Processed dataframe
        
    Returns:
        DataFrame: Final dataset ready for export
    """
    # Filter out rows with missing Last Name or First Name BEFORE any string operations
    df = filter_blank_values(df, ['Last Name', 'First Name'])
    
    # Now convert to string (should be safe since we filtered out NaN/empty)
    df['Last Name'] = df['Last Name'].astype(str)
    df['First Name'] = df['First Name'].astype(str)
    
    # Create Name column (Last, First format)
    df['Name'] = df['Last Name'] + ',' + df['First Name']
    
    # Filter out rows where Name is empty or just a comma (meaning one of the names was empty)
    df = df[df['Name'] != ',']
    df = df[df['Name'].str.strip() != '']
    
    # Convert Gender to Sex codes
    df['Sex'] = df['Gender'].map({
        'Male': 'M',
        'Female': 'F'
    }).fillna('U')
    
    # Truncate Name to 30 characters
    df['Name'] = df['Name'].str[:30]
    
    # Final check - remove any rows where Name became empty after truncation
    df = df[df['Name'].str.strip() != '']
    
    # Select final columns and rename
    final_cols = [
        'Person ID', 'Name', 'Date of Birth', 'Sex',
        'Last Name', 'First Name', 'Middle Name',
        'Entered Care', 'Exited Care'
    ]
    
    df_final = df[final_cols].copy()
    df_final = df_final.rename(columns={
        'Person ID': 'Person_ID',
        'Date of Birth': 'Date_of_Birth',
        'Last Name': 'Last_Name',
        'First Name': 'First_Name',
        'Middle Name': 'Middle_Name',
        'Entered Care': 'Entered_Care',
        'Exited Care': 'Exited_Care'
    })
    
    # Remove duplicates
    df_final = df_final.drop_duplicates()
    
    return df_final


def write_fixed_width_file(df, filename):
    """
    Write fixed-width text file for DPS submission.
    
    Format: Name (30 chars), Date_of_Birth (10 chars in YYYY-MM-DD format), Sex (1 char)
    No column headers, no separators.
    
    Args:
        df (DataFrame): Dataframe with Name, Date_of_Birth, Sex columns
        filename (str): Output filename
    """
    # 1. Select and deduplicate
    dps_df = df[['Name', 'Date_of_Birth', 'Sex']].drop_duplicates().copy()
    initial_count = len(dps_df)
    log.info(f"Starting with {initial_count} rows after deduplication")
    
    # 2. Convert dates (coerce invalid to NaT)
    dps_df['Date_of_Birth'] = pd.to_datetime(dps_df['Date_of_Birth'], errors='coerce')
    
    # 3. Filter out invalid rows - ALL validation in one place
    valid_mask = (
        dps_df['Name'].notna() & 
        dps_df['Date_of_Birth'].notna() &
        dps_df['Sex'].notna() &
        (dps_df['Name'].astype(str).str.strip().str.len() > 0) &
        (dps_df['Sex'].astype(str).str.strip().str.len() > 0)
    )
    
    dps_df = dps_df[valid_mask]
    log.info(f"After validation: {len(dps_df)} valid rows (removed {initial_count - len(dps_df)} invalid rows)")
    
    # 4. Format as strings with proper widths
    dps_df['Name'] = dps_df['Name'].astype(str).str.strip().str.slice(0, 30).str.ljust(30)
    dps_df['Date_of_Birth'] = dps_df['Date_of_Birth'].dt.strftime('%Y-%m-%d')  # Always 10 chars
    dps_df['Sex'] = dps_df['Sex'].astype(str).str.strip().str.slice(0, 1)
    
    # 5. Write to file (simple and clean!)
    with open(filename, 'w') as f:
        for _, row in dps_df.iterrows():
            f.write(f"{row['Name']}{row['Date_of_Birth']}{row['Sex']}\n")
    
    log.info(f"Fixed-width file written: {filename} ({len(dps_df)} rows)")



def main():
    """Main execution function."""
    global configs, envconfig, log
    
    parser = argparse.ArgumentParser(description="Send children in care data to DPS")
    parser.add_argument("env", choices=['dev', 'prod', 'qawh'], help="Environment to run")
    args = parser.parse_args()
    
    env = args.env
    configs, envconfig = read_config(env)
    log_dir = configs['log']['to_dps']
    log = get_logger("send2dps", "send2dps.log", log_dir)

    # Add delimiter line for this run
    delimiter = "=" * 80
    log.info(delimiter)
    log.info(f"Starting send2dps.py - Run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(delimiter)
    
    try:
        # Get current date
        run_date = date.today()
        
        # Get filtered data and query period
        filtered_data, query_period = get_filtered_data_for_period(run_date, envconfig)
        
        # Filter out rows with no enter care date
        filtered_data = filtered_data[filtered_data['DT_ENTER_CARE'].notna()]
        log.info(f"Rows after filtering null enter care dates: {len(filtered_data)}")
        
        # Also filter out rows with missing dates of birth early
        filtered_data = filtered_data[filtered_data['DT_CHILD_BIRTH'].notna()]
        log.info(f"Rows after filtering null date of birth: {len(filtered_data)}")
        
        # Process names
        log.info("Processing names...")
        processed_data = process_names(filtered_data)
        log.info(f"Rows after name processing: {len(processed_data)}")
        
        # Check for empty names after processing
        empty_names = processed_data[
            (processed_data['Last Name'].isna()) | 
            (processed_data['Last Name'] == '') |
            (processed_data['First Name'].isna()) | 
            (processed_data['First Name'] == '')
        ]
        if len(empty_names) > 0:
            log.warning(f"Found {len(empty_names)} rows with empty names after processing")
        
        # Create final dataset
        log.info("Creating final dataset...")
        final_data = create_final_dataset(processed_data)
        log.info(f"Final dataset rows: {len(final_data)}")
        
        # Check for empty names/dates in final dataset
        empty_final = final_data[
            (final_data['Name'].isna()) | 
            (final_data['Name'] == '') |
            (final_data['Name'].str.strip() == '') |
            (final_data['Date_of_Birth'].isna())
        ]
        if len(empty_final) > 0:
            log.warning(f"Found {len(empty_final)} rows with empty names/dates in final dataset")
            log.warning(f"Sample of empty rows: {empty_final[['Name', 'Date_of_Birth']].head()}")
        
        # Get file paths from config
        file_paths = get_file_paths(query_period, configs, 'to_dps')
        end_export = file_paths['cps_sent']
        
        # Generate text file path (same directory as Excel file)
        end_txt_file = end_export.parent / f"Children_in_care_{query_period}.txt"
        
        # Ensure output directory exists
        end_export.parent.mkdir(parents=True, exist_ok=True)
        
        log.info(f"Saving reference data to: {end_export}")
        log.info(f"Saving DPS text file to: {end_txt_file}")
        
        # Write Excel file
        final_data.to_excel(end_export, index=False)
        log.info(f"Excel file written: {end_export}")
        
        # Write fixed-width text file
        write_fixed_width_file(final_data, str(end_txt_file))
        
        # Upload text file to DPS via SFTP
        sftp, ssh = get_sftp_connection(configs)
        remote_dir = configs['sftp']['remote_dir_to_dps']
        log.info(f"Uploading {end_txt_file.name} to DPS SFTP...")
        upload_file_sftp(sftp, end_txt_file, remote_dir)
        log.info(f"Successfully uploaded {end_txt_file.name} to {remote_dir}")
        ssh.close()
        
        # Archive both files
        log.info("Archiving output files...")
        archive_file(end_export, 'to_dps', configs, query_period)
        archive_file(end_txt_file, 'to_dps', configs, query_period)
        
        log.info("send2dps.py completed successfully")
        log.info("=" * 80)
        
    except Exception as e:
        log.error(f"Exception: {e}")
        message = f"Error from send2dps.py for {env}\n"
        message += str(e) + "\n"
        # include the full stack trace in the error email
        message += traceback.format_exc()
        email_error(message, configs.get('error_email', 'brent.jones@dfps.texas.gov'), 
                   "send2dps.py", f"{log_dir}/send2dps.log")
        log.error("=" * 80)
        raise


if __name__ == "__main__":
    main()

