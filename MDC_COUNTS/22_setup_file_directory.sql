-- ============================================================================
-- Setup Directory Object for Validation Report Files
-- ============================================================================
-- Purpose: Create Oracle directory object for HTML file generation
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Directory Object
-- ============================================================================
-- Replace '/path/to/your/directory' with your actual directory path
-- Examples:
--   Linux/Unix: '/tmp/validation_reports' or '/home/oracle/validation_reports'
--   Windows: 'C:\temp\validation_reports' or 'D:\reports\validation'
-- ============================================================================

-- For Linux/Unix:
CREATE OR REPLACE DIRECTORY VALIDATION_REPORTS AS '/tmp/validation_reports';

-- For Windows (uncomment and modify as needed):
-- CREATE OR REPLACE DIRECTORY VALIDATION_REPORTS AS 'C:\temp\validation_reports';

-- ============================================================================
-- STEP 2: Grant Permissions
-- ============================================================================
-- Grant read/write access to your schema
-- Replace 'YOUR_SCHEMA' with your actual schema name (e.g., 'CAPS', 'MDC', etc.)
-- ============================================================================

-- Grant to your schema
GRANT READ, WRITE ON DIRECTORY VALIDATION_REPORTS TO YOUR_SCHEMA;

-- Or if you want to grant to PUBLIC (all users):
-- GRANT READ, WRITE ON DIRECTORY VALIDATION_REPORTS TO PUBLIC;

-- ============================================================================
-- STEP 3: Verify Directory Setup
-- ============================================================================

SELECT 
    directory_name,
    directory_path
FROM all_directories
WHERE directory_name = 'VALIDATION_REPORTS';

-- ============================================================================
-- STEP 4: Test Directory Access (Optional)
-- ============================================================================
-- Uncomment to test if Oracle can write to the directory
-- ============================================================================

/*
DECLARE
    v_file_handle UTL_FILE.FILE_TYPE;
BEGIN
    v_file_handle := UTL_FILE.FOPEN('VALIDATION_REPORTS', 'test.txt', 'W');
    UTL_FILE.PUT_LINE(v_file_handle, 'Test file created successfully');
    UTL_FILE.FCLOSE(v_file_handle);
    DBMS_OUTPUT.PUT_LINE('Directory access test: SUCCESS');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Directory access test: FAILED - ' || SQLERRM);
        IF v_file_handle IS NOT NULL THEN
            BEGIN
                UTL_FILE.FCLOSE(v_file_handle);
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END;
        END IF;
END;
/
*/

PROMPT
PROMPT ============================================================================
PROMPT Directory Setup Complete
PROMPT ============================================================================
PROMPT
PROMPT IMPORTANT: Make sure the OS directory exists and Oracle has write permissions
PROMPT
PROMPT To create the directory on Linux/Unix:
PROMPT   mkdir -p /tmp/validation_reports
PROMPT   chmod 777 /tmp/validation_reports
PROMPT
PROMPT To create the directory on Windows:
PROMPT   mkdir C:\temp\validation_reports
PROMPT   (Grant write permissions to Oracle service account)
PROMPT

