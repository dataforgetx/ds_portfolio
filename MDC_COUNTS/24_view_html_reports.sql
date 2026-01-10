-- ============================================================================
-- View HTML Validation Reports
-- ============================================================================
-- Purpose: Query and export HTML validation reports from database table
-- ============================================================================

SET PAGESIZE 0;
SET LONG 1000000;
SET LONGCHUNKSIZE 1000000;
SET LINESIZE 32767;

-- ============================================================================
-- Option 1: View all reports for today
-- ============================================================================

PROMPT ============================================================================
PROMPT Reports for Today
PROMPT ============================================================================

SELECT 
    email_address,
    TO_CHAR(run_date, 'YYYY-MM-DD HH24:MI:SS') AS run_date,
    table_count,
    TO_CHAR(DBMS_LOB.GETLENGTH(html_content)) || ' chars' AS content_size,
    TO_CHAR(created_date, 'YYYY-MM-DD HH24:MI:SS') AS created_date
FROM TBL_MDC_VALIDATION_HTML_REPORTS
WHERE TRUNC(run_date) = TRUNC(SYSDATE)
ORDER BY email_address;

-- ============================================================================
-- Option 2: View HTML content for a specific analyst
-- ============================================================================
-- Replace 'your.email@example.com' with actual email address
-- ============================================================================

/*
SELECT 
    email_address,
    TO_CHAR(run_date, 'YYYY-MM-DD HH24:MI:SS') AS run_date,
    table_count,
    html_content
FROM TBL_MDC_VALIDATION_HTML_REPORTS
WHERE email_address = 'your.email@example.com'
  AND TRUNC(run_date) = TRUNC(SYSDATE);
*/

-- ============================================================================
-- Option 3: Export HTML to file using SQL*Plus SPOOL
-- ============================================================================
-- Uncomment and modify as needed
-- ============================================================================

/*
-- Set spool file
SPOOL /tmp/validation_report.html

-- Get HTML content
SELECT html_content
FROM TBL_MDC_VALIDATION_HTML_REPORTS
WHERE email_address = 'your.email@example.com'
  AND TRUNC(run_date) = TRUNC(SYSDATE);

SPOOL OFF
*/

-- ============================================================================
-- Option 4: View all reports with summary
-- ============================================================================

PROMPT
PROMPT ============================================================================
PROMPT Summary of All Reports
PROMPT ============================================================================

SELECT 
    TO_CHAR(run_date, 'YYYY-MM-DD') AS report_date,
    COUNT(*) AS report_count,
    SUM(table_count) AS total_tables,
    SUM(DBMS_LOB.GETLENGTH(html_content)) AS total_size_chars
FROM TBL_MDC_VALIDATION_HTML_REPORTS
GROUP BY TO_CHAR(run_date, 'YYYY-MM-DD')
ORDER BY TO_CHAR(run_date, 'YYYY-MM-DD') DESC;

PROMPT
PROMPT ============================================================================
PROMPT To export HTML content:
PROMPT   1. Use SQL Developer: Right-click on html_content column -> Export
PROMPT   2. Use SQL*Plus SPOOL command (see Option 3 above)
PROMPT   3. Query the table and copy/paste the HTML content
PROMPT ============================================================================
PROMPT

