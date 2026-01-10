-- ============================================================================
-- Test Full Validation for Specific Tables (Including Email Sending)
-- ============================================================================
-- Purpose: Test validation for a few tables and send emails
-- ============================================================================

SET SERVEROUTPUT ON SIZE 1000000;
SET LINESIZE 200;
SET PAGESIZE 0;
SET FEEDBACK OFF;

-- ============================================================================
-- STEP 1: Run validation for specific tables
-- ============================================================================
-- This will run all validation types configured for each table
-- ============================================================================

PROMPT ============================================================================
PROMPT STEP 1: Running validation for specific tables
PROMPT ============================================================================
PROMPT

DECLARE
    -- List of tables to test (modify as needed)
    TYPE t_table_tab IS TABLE OF VARCHAR2(128) INDEX BY PLS_INTEGER;
    v_tables t_table_tab;
    v_owner VARCHAR2(30) := 'CAPS';
    v_error_count NUMBER := 0;
BEGIN
    -- Add tables to test - modify this list as needed
    v_tables(1) := 'FAD_SUM';
    v_tables(2) := 'FAD_FACT';
    -- Add more tables as needed:
    -- v_tables(3) := 'SVC_SUM';
    -- v_tables(4) := 'INT_SUM';
    
    DBMS_OUTPUT.PUT_LINE('Testing validation for ' || v_tables.COUNT || ' table(s)');
    DBMS_OUTPUT.PUT_LINE('============================================================================');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Run validation for each table
    FOR i IN 1..v_tables.COUNT LOOP
        BEGIN
            DBMS_OUTPUT.PUT_LINE('Running validation for: ' || v_owner || '.' || v_tables(i));
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            
            PKG_MDC_VALIDATION.RUN_TABLE_VALIDATION(
                p_owner => v_owner,
                p_table_name => v_tables(i),
                p_time_load_start => NULL,  -- Use defaults from config
                p_time_load_end => NULL     -- Use defaults from config
            );
            
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('✓ Completed: ' || v_owner || '.' || v_tables(i));
            DBMS_OUTPUT.PUT_LINE('');
            
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('✗ ERROR validating ' || v_owner || '.' || v_tables(i) || ': ' || SQLERRM);
                DBMS_OUTPUT.PUT_LINE('');
        END;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('============================================================================');
    DBMS_OUTPUT.PUT_LINE('Table validation summary:');
    DBMS_OUTPUT.PUT_LINE('  Tables processed: ' || v_tables.COUNT);
    DBMS_OUTPUT.PUT_LINE('  Errors: ' || v_error_count);
    DBMS_OUTPUT.PUT_LINE('============================================================================');
    DBMS_OUTPUT.PUT_LINE('');
    
END;
/

PROMPT
PROMPT ============================================================================
PROMPT STEP 2: Diagnostic - Check if results exist with email addresses
PROMPT ============================================================================
PROMPT

SELECT 
    'Total results for today' AS check_type,
    COUNT(*) AS count_value
FROM TBL_MDC_VALIDATION_RESULTS
WHERE TRUNC(run_date) = TRUNC(SYSDATE)
UNION ALL
SELECT 
    'Results with email addresses' AS check_type,
    COUNT(*) AS count_value
FROM TBL_MDC_VALIDATION_RESULTS
WHERE TRUNC(run_date) = TRUNC(SYSDATE)
  AND email_address IS NOT NULL
UNION ALL
SELECT 
    'Distinct email addresses' AS check_type,
    COUNT(DISTINCT email_address) AS count_value
FROM TBL_MDC_VALIDATION_RESULTS
WHERE TRUNC(run_date) = TRUNC(SYSDATE)
  AND email_address IS NOT NULL;

PROMPT
PROMPT If the counts above are 0, emails will not be sent.
PROMPT Check:
PROMPT   1. Did validation run successfully? (see Step 1 output)
PROMPT   2. Are email addresses set in TBL_MDC_VALIDATION_CONFIG?
PROMPT   3. Are results being inserted with email_address populated?
PROMPT

PROMPT
PROMPT Checking email addresses in config for test tables:
PROMPT

SELECT 
    owner,
    table_name,
    validation_type,
    email_address,
    is_active
FROM TBL_MDC_VALIDATION_CONFIG
WHERE owner = 'CAPS'  -- Change if testing different owner
  AND table_name IN ('FAD_SUM', 'FAD_FACT')  -- Change to match your test tables
ORDER BY table_name, validation_type;

PROMPT
PROMPT ============================================================================
PROMPT STEP 3: Store HTML reports in database table (No directory permissions needed)
PROMPT ============================================================================
PROMPT

BEGIN
    DBMS_OUTPUT.PUT_LINE('Storing HTML reports in database table...');
    DBMS_OUTPUT.PUT_LINE('Using run_date: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('');
    
    -- First, check if there are any results
    DECLARE
        v_result_count NUMBER;
        v_email_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_result_count
        FROM TBL_MDC_VALIDATION_RESULTS
        WHERE TRUNC(run_date) = TRUNC(SYSDATE);
        
        SELECT COUNT(DISTINCT email_address)
        INTO v_email_count
        FROM TBL_MDC_VALIDATION_RESULTS
        WHERE TRUNC(run_date) = TRUNC(SYSDATE)
          AND email_address IS NOT NULL;
        
        DBMS_OUTPUT.PUT_LINE('Results found: ' || v_result_count);
        DBMS_OUTPUT.PUT_LINE('Email addresses with results: ' || v_email_count);
        DBMS_OUTPUT.PUT_LINE('');
        
        IF v_result_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('⚠ WARNING: No validation results found for today.');
            DBMS_OUTPUT.PUT_LINE('  Make sure Step 1 completed successfully.');
            DBMS_OUTPUT.PUT_LINE('  Check if tables exist and have data.');
            RETURN;
        END IF;
        
        IF v_email_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('⚠ WARNING: No results have email addresses assigned.');
            DBMS_OUTPUT.PUT_LINE('  Update TBL_MDC_VALIDATION_CONFIG.EMAIL_ADDRESS for your test tables.');
            DBMS_OUTPUT.PUT_LINE('  Example:');
            DBMS_OUTPUT.PUT_LINE('    UPDATE TBL_MDC_VALIDATION_CONFIG');
            DBMS_OUTPUT.PUT_LINE('    SET email_address = ''your.email@example.com''');
            DBMS_OUTPUT.PUT_LINE('    WHERE owner = ''CAPS'' AND table_name IN (''FAD_SUM'', ''FAD_FACT'');');
            RETURN;
        END IF;
    END;
    
    -- Store HTML reports in database table
    PKG_MDC_VALIDATION.STORE_VALIDATION_REPORTS(p_run_date => SYSDATE);
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('✓ Report storage completed');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ ERROR storing reports: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('  SQLCODE: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('  SQLERRM: ' || SQLERRM);
END;
/

PROMPT
PROMPT ============================================================================
PROMPT Alternative: Send emails (if you want to try emailing instead)
PROMPT ============================================================================
PROMPT Uncomment the block below to send emails instead of generating files
PROMPT

/*
BEGIN
    PKG_MDC_VALIDATION.SEND_VALIDATION_EMAILS(p_run_date => SYSDATE);
END;
/
*/

PROMPT
PROMPT ============================================================================
PROMPT Alternative: Run full validation for all tables (use with caution)
PROMPT ============================================================================
PROMPT Uncomment the block below to run validation for ALL configured tables
PROMPT

/*
BEGIN
    PKG_MDC_VALIDATION.RUN_FULL_VALIDATION(
        p_spool_file => NULL,              -- No spool file
        p_time_load_start => NULL,         -- Use defaults
        p_time_load_end => NULL,           -- Use defaults
        p_generate_emails => 'Y'          -- Send emails automatically
    );
END;
/
*/

-- ============================================================================
-- Verify Results
-- ============================================================================

PROMPT
PROMPT ============================================================================
PROMPT Verification: Check validation results
PROMPT ============================================================================
PROMPT

SELECT 
    owner || '.' || table_name AS table_name,
    validation_type,
    COUNT(*) AS result_count,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed
FROM TBL_MDC_VALIDATION_RESULTS
WHERE TRUNC(run_date) = TRUNC(SYSDATE)
GROUP BY owner, table_name, validation_type
ORDER BY owner, table_name, validation_type;

PROMPT
PROMPT ============================================================================
PROMPT Verification: Check email addresses that will receive emails
PROMPT ============================================================================
PROMPT

SELECT 
    email_address,
    COUNT(DISTINCT owner || '.' || table_name) AS table_count,
    COUNT(*) AS validation_count,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings
FROM TBL_MDC_VALIDATION_RESULTS
WHERE TRUNC(run_date) = TRUNC(SYSDATE)
  AND email_address IS NOT NULL
GROUP BY email_address
ORDER BY email_address;

PROMPT
PROMPT ============================================================================
PROMPT Test Complete
PROMPT ============================================================================
PROMPT
PROMPT To view detailed results:
PROMPT SELECT * FROM TBL_MDC_VALIDATION_RESULTS 
PROMPT WHERE TRUNC(run_date) = TRUNC(SYSDATE)
PROMPT ORDER BY owner, table_name, validation_type, time_load;
PROMPT

