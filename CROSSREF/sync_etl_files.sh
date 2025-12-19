#!/bin/bash
########################################################################
# sync_etl_files.sh
# Syncs ETL files from /usr/app/prodware into Oracle staging table
# 
# Usage: ./sync_etl_files.sh
# Environment variables should be set before running:
#   USER_NAME, DB, USER_PASSWORD (via op command)
# Author: Hanlong Fu
# Date: 12/17/2025
########################################################################

# Oracle Environment Variables
export PATH=/usr/local/bin:$PATH
export ORACLE_HOME=`dbhome default`
export LD_LIBRARY_PATH=$ORACLE_HOME/lib

# Database connection from environment variables
export USER_NAME="${USER_NAME:-caps}"
export DB="${DB:-qawh}"
export USER_PASSWORD="${USER_PASSWORD:-`op -g dba $USER_NAME@$DB`}"

# Configuration - limit search to sql and bin subdirectories
ETL_DIR="/usr/app/prodware"
ETL_SQL_DIR="${ETL_DIR}/sql"
ETL_BIN_DIR="${ETL_DIR}/bin"
SCRIPT_DIR="/rhome/fuh2/mrs/crossref2"
EXPORT_FILE="${SCRIPT_DIR}/etl_files_export.dat"
CTL_FILE="${SCRIPT_DIR}/etl_files.ctl"
LOG_FILE="${SCRIPT_DIR}/logs/etl_sync.log"
BAD_FILE="${SCRIPT_DIR}/etl_files_export.bad"
DSC_FILE="${SCRIPT_DIR}/etl_files_export.dsc"

# Create log directory if it doesn't exist
mkdir -p "${SCRIPT_DIR}/logs"

# Logging function
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

# Error handling
error_exit() {
    log_message "ERROR: $1"
    exit 1
}

# Start sync
log_message "=========================================="
log_message "Starting ETL files sync from ${ETL_SQL_DIR} and ${ETL_BIN_DIR}"
log_message "Database: ${USER_NAME}@${DB}"
log_message "ORACLE_HOME: ${ORACLE_HOME}"

# Check if ETL directories exist
if [ ! -d "${ETL_SQL_DIR}" ]; then
    error_exit "ETL SQL directory ${ETL_SQL_DIR} does not exist or is not accessible"
fi
if [ ! -d "${ETL_BIN_DIR}" ]; then
    error_exit "ETL BIN directory ${ETL_BIN_DIR} does not exist or is not accessible"
fi

# Check if control file exists
if [ ! -f "${CTL_FILE}" ]; then
    error_exit "Control file ${CTL_FILE} not found"
fi

# Remove old export file if it exists
if [ -f "${EXPORT_FILE}" ]; then
    rm -f "${EXPORT_FILE}"
    log_message "Removed old export file"
fi

# Remove old bad and discard files
rm -f "${BAD_FILE}" "${DSC_FILE}"

# Initialize counters
file_count=0
line_count=0
error_count=0

# Generate timestamp once (used for all rows)
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Function to process a single file using awk (much faster than bash loops)
process_file() {
    local file="$1"
    local filetype="$2"
    local filename=$(basename "$file")
    local filepath="$file"
    
    # Use awk for fast line processing (single process, no subprocess calls)
    awk -v filename="$filename" \
        -v filepath="$filepath" \
        -v filetype="$filetype" \
        -v timestamp="$TIMESTAMP" \
        '{
            # Escape pipe characters and truncate to 4000 chars in one step
            line = $0
            gsub(/\|/, " ", line)
            if (length(line) > 4000) line = substr(line, 1, 4000)
            
            # Output pipe-delimited line
            print filename "|" filepath "|" NR "|" line "|" filetype "|" timestamp
        }' "$file" >> "${EXPORT_FILE}"
    
    # Count lines from this file
    local lines=$(wc -l < "$file" 2>/dev/null || echo 0)
    echo $lines
}

# Find and process SQL files from sql and bin directories
log_message "Scanning for .sql files in ${ETL_SQL_DIR} and ${ETL_BIN_DIR}..."
start_time=$(date +%s)
while IFS= read -r -d '' file; do
    lines_added=$(process_file "$file" "SQL")
    line_count=$((line_count + lines_added))
    file_count=$((file_count + 1))
    
    if [ $((file_count % 500)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        log_message "Processed ${file_count} SQL files, ${line_count} lines (${elapsed}s elapsed)..."
    fi
done < <(find "${ETL_SQL_DIR}" "${ETL_BIN_DIR}" -type f -name "*.sql" -print0 2>/dev/null)

sql_time=$(($(date +%s) - start_time))
log_message "SQL files complete: ${file_count} files, ${line_count} lines in ${sql_time}s"

# Find and process shell script files from sql and bin directories
log_message "Scanning for .sh, .ksh, .bash files in ${ETL_SQL_DIR} and ${ETL_BIN_DIR}..."
start_time=$(date +%s)
while IFS= read -r -d '' file; do
    lines_added=$(process_file "$file" "SH")
    line_count=$((line_count + lines_added))
    file_count=$((file_count + 1))
    
    if [ $((file_count % 100)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        log_message "Processed ${file_count} shell files, ${line_count} lines (${elapsed}s elapsed)..."
    fi
done < <(find "${ETL_SQL_DIR}" "${ETL_BIN_DIR}" -type f \( -name "*.sh" -o -name "*.ksh" -o -name "*.bash" \) -print0 2>/dev/null)

shell_time=$(($(date +%s) - start_time))
log_message "Shell files complete: ${file_count} total files, ${line_count} total lines (shell processing: ${shell_time}s)"

log_message "File scanning complete: ${file_count} files, ${line_count} lines"

# Truncate staging table before loading
log_message "Truncating staging table..."
sqlplus -s "${USER_NAME}/${USER_PASSWORD}@${DB}" <<EOF >> "${LOG_FILE}" 2>&1
SET PAGESIZE 0
SET FEEDBACK OFF
SET VERIFY OFF
TRUNCATE TABLE ram.a\$ETL_Files_Staging;
EXIT;
EOF

if [ $? -ne 0 ]; then
    error_exit "Failed to truncate staging table"
fi

log_message "Staging table truncated"

# Load data using SQL*Loader
log_message "Loading data into staging table using SQL*Loader..."
sqlldr "${USER_NAME}/${USER_PASSWORD}@${DB}" \
    control="${CTL_FILE}" \
    log="${SCRIPT_DIR}/logs/etl_loader.log" \
    bad="${BAD_FILE}" \
    discard="${DSC_FILE}" \
    silent=HEADER,FEEDBACK

loader_exit=$?

# Check for errors
if [ -f "${BAD_FILE}" ] && [ -s "${BAD_FILE}" ]; then
    bad_count=$(wc -l < "${BAD_FILE}")
    log_message "WARNING: ${bad_count} rows rejected (see ${BAD_FILE})"
    error_count=$((error_count + bad_count))
fi

if [ $loader_exit -ne 0 ]; then
    error_exit "SQL*Loader failed with exit code ${loader_exit}. Check ${SCRIPT_DIR}/logs/etl_loader.log"
fi

# Get row count from database
log_message "Verifying load..."
row_count=$(sqlplus -s "${USER_NAME}/${USER_PASSWORD}@${DB}" <<EOF
SET PAGESIZE 0
SET FEEDBACK OFF
SET VERIFY OFF
SELECT COUNT(*) FROM ram.a\$ETL_Files_Staging;
EXIT;
EOF
)

log_message "Load complete: ${row_count} rows in staging table"

# Cleanup export file (optional - comment out if you want to keep it for debugging)
# rm -f "${EXPORT_FILE}"
# log_message "Cleaned up export file"

# Summary
log_message "=========================================="
log_message "Sync Summary:"
log_message "  Files processed: ${file_count}"
log_message "  Lines exported: ${line_count}"
log_message "  Rows loaded: ${row_count}"
if [ $error_count -gt 0 ]; then
    log_message "  Errors: ${error_count}"
fi
log_message "Sync completed successfully"
log_message "=========================================="

exit 0

