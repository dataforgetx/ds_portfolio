-- ============================================================================
-- MDC Validation System - Run Validation for a Specific Table
-- ============================================================================
-- Purpose: Example script showing how to run validation for a single table
-- ============================================================================

-- ============================================================================
-- METHOD 1: Run All Validations for a Specific Table (RECOMMENDED)
-- ============================================================================
-- This is the simplest way - just provide owner and table name
-- The procedure automatically identifies and runs all validation types
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED;

BEGIN
    PKG_MDC_VALIDATION.RUN_TABLE_VALIDATION(
        p_owner => 'CAPS',              -- Change to your table owner
        p_table_name => 'SA_SUBCARE_FACT',  -- Change to your table name
        p_time_load_start => NULL,      -- NULL = use defaults from config
        p_time_load_end => NULL        -- NULL = use defaults from config
    );
END;
/

-- ============================================================================
-- METHOD 2: Run with Custom Time Load Range
-- ============================================================================
-- Override the default time load range if needed
-- ============================================================================

/*
SET SERVEROUTPUT ON SIZE UNLIMITED;

BEGIN
    PKG_MDC_VALIDATION.RUN_TABLE_VALIDATION(
        p_owner => 'CAPS',
        p_table_name => 'SA_SUBCARE_FACT',
        p_time_load_start => 100250,  -- Custom start time load
        p_time_load_end => 100252     -- Custom end time load
    );
END;
/
*/

-- ============================================================================
-- METHOD 3: Check What Validations Are Configured for a Table
-- ============================================================================
-- Run this first to see what validations will be executed
-- ============================================================================

/*
SELECT config_id,
       owner,
       table_name,
       validation_type,
       IS_ACTIVE,
       THRESHOLD_PCT,
       GROUP_BY_COLUMNS,
       COMPARE_TO_TABLE,
       EMAIL_ADDRESS,
       NOTES
FROM TBL_MDC_VALIDATION_CONFIG
WHERE owner = 'CAPS'  -- Change to your owner
  AND table_name = 'SA_SUBCARE_FACT'  -- Change to your table name
ORDER BY validation_type;
*/

-- ============================================================================
-- METHOD 4: Run Full Validation (All Tables)
-- ============================================================================
-- Run validations for all configured tables
-- ============================================================================

/*
SET SERVEROUTPUT ON SIZE UNLIMITED;

BEGIN
    PKG_MDC_VALIDATION.RUN_FULL_VALIDATION;
    COMMIT;
END;
/
*/

-- ============================================================================
-- METHOD 5: View Results for a Table
-- ============================================================================
-- Query validation results after running
-- ============================================================================

/*
SELECT validation_type,
       time_load,
       column_name,
       count_value,
       prior_count,
       pct_change,
       status,
       severity,
       message,
       run_date
FROM TBL_MDC_VALIDATION_RESULTS
WHERE owner = 'CAPS'  -- Change to your owner
  AND table_name = 'SA_SUBCARE_FACT'  -- Change to your table name
  AND TRUNC(run_date) = TRUNC(SYSDATE)
ORDER BY validation_type, time_load, severity DESC;
*/

