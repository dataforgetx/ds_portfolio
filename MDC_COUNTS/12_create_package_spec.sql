-- ============================================================================
-- MDC Validation System - Package Specification
-- ============================================================================
-- Purpose: Define the public interface for PKG_MDC_VALIDATION package
-- ============================================================================

CREATE OR REPLACE PACKAGE PKG_MDC_VALIDATION AS

    -- ========================================================================
    -- Main Procedure
    -- ========================================================================
    
    /**
     * Main procedure to run all validations
     * 
     * @param p_spool_file Optional spool file path for output
     * @param p_time_load_start Optional start time load (overrides config defaults)
     * @param p_time_load_end Optional end time load (overrides config defaults)
     * @param p_generate_emails Flag to generate email files (Y/N, default Y)
     */
    PROCEDURE RUN_FULL_VALIDATION(
        p_spool_file          IN VARCHAR2 DEFAULT NULL,
        p_time_load_start     IN NUMBER DEFAULT NULL,
        p_time_load_end       IN NUMBER DEFAULT NULL,
        p_generate_emails     IN VARCHAR2 DEFAULT 'Y'
    );
    
    /**
     * Run all validations for a specific table
     * Automatically identifies and runs all validation types configured for the table
     * 
     * @param p_owner Table owner (e.g., 'CAPS', 'CCL', 'SWI')
     * @param p_table_name Table name to validate
     * @param p_time_load_start Optional start time load (overrides config defaults)
     * @param p_time_load_end Optional end time load (overrides config defaults)
     */
    PROCEDURE RUN_TABLE_VALIDATION(
        p_owner           IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_time_load_start IN NUMBER DEFAULT NULL,
        p_time_load_end   IN NUMBER DEFAULT NULL
    );

    -- ========================================================================
    -- Time Load Management Procedures
    -- ========================================================================
    
    /**
     * Get the current time load from TIME_DIM
     * @return Current time load ID
     */
    FUNCTION GET_CURRENT_TIME_LOAD RETURN NUMBER;
    
    /**
     * Calculate base time load based on current time load and special cases
     * Handles November/October special cases for WARE/QAWH
     * @param p_current_tl Current time load
     * @return Base time load (typically CurrentTL-2, or CurrentTL-13 for Nov/Oct)
     */
    FUNCTION CALCULATE_BASE_TIME_LOAD(p_current_tl IN NUMBER) RETURN NUMBER;
    
    /**
     * Calculate time load ranges for a specific table
     * Uses config table TIME_LOAD_RANGE_START/END if specified, otherwise uses defaults
     * @param p_owner Table owner
     * @param p_table_name Table name
     * @param p_current_tl Current time load
     * @param p_base_tl Base time load
     * @param p_time_load_start OUT Start time load for validation
     * @param p_time_load_end OUT End time load for validation
     */
    PROCEDURE CALCULATE_TIME_LOAD_RANGES(
        p_owner           IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_current_tl      IN NUMBER,
        p_base_tl         IN NUMBER,
        p_time_load_start OUT NUMBER,
        p_time_load_end   OUT NUMBER
    );

    -- ========================================================================
    -- Validation Execution Procedures
    -- ========================================================================
    
    /**
     * Execute row count validation for a table
     * Handles basic row counts, grouped row counts, and dt_load_process grouping
     * @param p_config_id Configuration ID from TBL_MDC_VALIDATION_CONFIG
     * @param p_time_load_start Start time load
     * @param p_time_load_end End time load
     */
    PROCEDURE EXECUTE_ROW_COUNT_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    );
    
    /**
     * Execute column count validation for a table
     * Counts non-null values per column per time load
     * @param p_config_id Configuration ID from TBL_MDC_VALIDATION_CONFIG
     * @param p_time_load_start Start time load
     * @param p_time_load_end End time load
     */
    PROCEDURE EXECUTE_COLUMN_COUNT_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    );
    
    /**
     * Execute table comparison validation
     * Compares two related tables (e.g., FAD_SUM vs FAD_FACT)
     * Supports WHERE clause filters and dt_load_process grouping
     * @param p_config_id Configuration ID from TBL_MDC_VALIDATION_CONFIG
     * @param p_time_load_start Start time load
     * @param p_time_load_end End time load
     */
    PROCEDURE EXECUTE_TABLE_COMPARISON(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    );
    
    /**
     * Execute distinct count validation for a table
     * Counts distinct values in a column (e.g., COUNT(DISTINCT id_pp_person))
     * Supports optional CASE WHEN conditions
     * @param p_config_id Configuration ID from TBL_MDC_VALIDATION_CONFIG
     * @param p_time_load_start Start time load
     * @param p_time_load_end End time load
     */
    PROCEDURE EXECUTE_DISTINCT_COUNT_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    );
    
    /**
     * Execute data validation check
     * Performs row-by-row data validation using MINUS operations (for MDC_ALL comparisons)
     * @param p_config_id Configuration ID from TBL_MDC_VALIDATION_CONFIG
     * @param p_time_load_start Start time load
     * @param p_time_load_end End time load
     */
    PROCEDURE EXECUTE_DATA_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    );

    -- ========================================================================
    -- Existing mdc_counts.sql Validations
    -- ========================================================================
    
    /**
     * Check for tables without statistics
     */
    PROCEDURE CHECK_TABLES_WITHOUT_STATS;
    
    /**
     * Check for indexes without statistics
     */
    PROCEDURE CHECK_INDEXES_WITHOUT_STATS;
    
    /**
     * Check for invalid objects
     */
    PROCEDURE CHECK_INVALID_OBJECTS;
    
    /**
     * Compare table counts to ARCH table counts
     */
    PROCEDURE CHECK_ARCH_TABLE_COMPARISONS;
    
    /**
     * Check data volume consistency using MRS.ConsistentDataVolume
     */
    PROCEDURE CHECK_DATA_VOLUME_CONSISTENCY;
    
    /**
     * Get current time load counts for all tables
     */
    PROCEDURE GET_CURRENT_TIME_LOAD_COUNTS;
    
    /**
     * Compare CCL.FACILITY_FACT vs CCL.CCL_FACILITY_SUM
     */
    PROCEDURE CHECK_CCL_FACILITY_COMPARISON;

    -- ========================================================================
    -- Calculation and Processing Procedures
    -- ========================================================================
    
    /**
     * Calculate percentage change between current and prior counts
     * @param p_current_count Current count value
     * @param p_prior_count Prior count value
     * @return Percentage change (NULL if prior_count is 0)
     */
    FUNCTION CALCULATE_PERCENTAGE_CHANGE(
        p_current_count IN NUMBER,
        p_prior_count   IN NUMBER
    ) RETURN NUMBER;
    
    /**
     * Apply threshold rules and determine if violation occurred
     * @param p_pct_change Percentage change
     * @param p_threshold_pct Threshold percentage from config
     * @return TRUE if violation (pct_change > threshold), FALSE otherwise
     */
    FUNCTION IS_THRESHOLD_VIOLATION(
        p_pct_change   IN NUMBER,
        p_threshold_pct IN NUMBER
    ) RETURN BOOLEAN;
    
    /**
     * Assign status and severity based on percentage change and threshold
     * @param p_pct_change Percentage change
     * @param p_threshold_pct Threshold percentage
     * @param p_status OUT Status: PASS, WARNING, ERROR
     * @param p_severity OUT Severity: HIGH, MEDIUM, LOW
     */
    PROCEDURE ASSIGN_STATUS_SEVERITY(
        p_pct_change    IN NUMBER,
        p_threshold_pct IN NUMBER,
        p_status       OUT VARCHAR2,
        p_severity     OUT VARCHAR2
    );

    -- ========================================================================
    -- Results Storage Procedures
    -- ========================================================================
    
    /**
     * Insert validation result into TBL_MDC_VALIDATION_RESULTS
     * @param p_owner Table owner
     * @param p_table_name Table name
     * @param p_validation_type Validation type
     * @param p_time_load Time load
     * @param p_column_name Column name (for column count validations)
     * @param p_group_by_value Group by value (for grouped validations)
     * @param p_count_value Count value
     * @param p_prior_count Prior count value
     * @param p_pct_change Percentage change
     * @param p_status Status (PASS/WARNING/ERROR)
     * @param p_severity Severity (HIGH/MEDIUM/LOW)
     * @param p_message Message
     * @param p_email_address Email address from config
     * @param p_compare_table Comparison table name (for table comparisons)
     * @param p_compare_count Comparison table count (for table comparisons)
     * @param p_match_status Match status (MATCH/NO_MATCH/DOES_NOT_EXIST)
     */
    PROCEDURE INSERT_VALIDATION_RESULT(
        p_owner          IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_validation_type IN VARCHAR2,
        p_time_load      IN NUMBER,
        p_column_name    IN VARCHAR2 DEFAULT NULL,
        p_group_by_value IN VARCHAR2 DEFAULT NULL,
        p_count_value    IN NUMBER,
        p_prior_count    IN NUMBER DEFAULT NULL,
        p_pct_change     IN NUMBER DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_severity       IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_email_address  IN VARCHAR2 DEFAULT NULL,
        p_compare_table  IN VARCHAR2 DEFAULT NULL,
        p_compare_count  IN NUMBER DEFAULT NULL,
        p_match_status   IN VARCHAR2 DEFAULT NULL,
        p_avg_12month_count IN NUMBER DEFAULT NULL,
        p_pct_change_12month IN NUMBER DEFAULT NULL
    );

    -- ========================================================================
    -- Email Generation Procedures
    -- ========================================================================
    
    /**
     * Send validation results emails to all analysts
     * Groups results by email address and sends HTML formatted emails
     * @param p_run_date Optional run date (defaults to today)
     */
    PROCEDURE SEND_VALIDATION_EMAILS(
        p_run_date IN DATE DEFAULT NULL
    );
    
    /**
     * Generate HTML files for validation results instead of sending emails
     * Creates one HTML file per analyst in the specified directory
     * @param p_directory Directory path for output files (e.g., '/tmp/validation_reports' or 'C:\temp\validation_reports')
     * @param p_run_date Optional run date (defaults to today)
     */
    PROCEDURE GENERATE_VALIDATION_FILES(
        p_directory IN VARCHAR2,
        p_run_date IN DATE DEFAULT NULL
    );
    
    /**
     * Store HTML validation reports in database table (no directory permissions needed)
     * Creates one HTML report per analyst and stores in TBL_MDC_VALIDATION_HTML_REPORTS
     * @param p_run_date Optional run date (defaults to today)
     */
    PROCEDURE STORE_VALIDATION_REPORTS(
        p_run_date IN DATE DEFAULT NULL
    );
    
    /**
     * Generate HTML email content for a specific analyst
     * @param p_email_address Analyst email address
     * @param p_run_date Run date for filtering results
     * @param p_html_content OUT Generated HTML content
     */
    PROCEDURE GENERATE_EMAIL_CONTENT(
        p_email_address IN VARCHAR2,
        p_run_date      IN DATE,
        p_html_content  OUT CLOB
    );
    
    /**
     * Generate HTML email content for a specific analyst filtered by validation type
     * Used when splitting large emails into multiple parts
     * @param p_email_address Analyst email address
     * @param p_run_date Run date for filtering results
     * @param p_validation_type Validation type to include (NULL = all types)
     * @param p_part_number Part number for multi-part emails
     * @param p_total_parts Total number of parts
     * @param p_html_content OUT Generated HTML content
     */
    PROCEDURE GENERATE_EMAIL_CONTENT_BY_TYPE(
        p_email_address    IN VARCHAR2,
        p_run_date         IN DATE,
        p_validation_type   IN VARCHAR2,
        p_part_number      IN NUMBER,
        p_total_parts      IN NUMBER,
        p_html_content     OUT CLOB
    );

    -- ========================================================================
    -- Utility Procedures
    -- ========================================================================
    
    /**
     * Get database name (hardcoded - QA or MAIN)
     * @return Database name: 'QA' or 'MAIN'
     */
    FUNCTION GET_DATABASE_NAME RETURN VARCHAR2;
    
    /**
     * Validate that a table exists in the database
     * @param p_owner Table owner
     * @param p_table_name Table name
     * @return TRUE if table exists, FALSE otherwise
     */
    FUNCTION VALIDATE_TABLE_EXISTS(
        p_owner      IN VARCHAR2,
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    /**
     * Get load process column name for a table
     * Handles special cases like dt_inv_load_process, dt_inv_afc_load_process, etc.
     * @param p_owner Table owner
     * @param p_table_name Table name
     * @return Load process column name (default: dt_load_process)
     */
    FUNCTION GET_LOAD_PROCESS_COLUMN(
        p_owner      IN VARCHAR2,
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2;
    
    /**
     * Clear results table for a specific run date
     * @param p_run_date Run date to clear (NULL clears all)
     */
    PROCEDURE CLEAR_RESULTS(
        p_run_date IN DATE DEFAULT NULL
    );

END PKG_MDC_VALIDATION;
/