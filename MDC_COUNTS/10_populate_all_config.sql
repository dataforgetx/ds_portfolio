-- ============================================================================
-- MDC Validation System - Complete Configuration Population Script
-- ============================================================================
-- Purpose: Populates configuration table with all tables from all schemas
--          Run this script after creating the database objects
-- ============================================================================

SET ECHO ON
SET FEEDBACK ON
SET VERIFY OFF

PROMPT ============================================================================
PROMPT Populating MDC Validation Configuration Table
PROMPT ============================================================================

PROMPT
PROMPT Step 1: Populating CAPS schema tables (Part 1)...
@05_populate_config_caps.sql

PROMPT
PROMPT Step 2: Populating CAPS schema tables (Part 2)...
@06_populate_config_caps_remaining.sql

PROMPT
PROMPT Step 3: Populating CAPS schema tables (Part 3)...
@07_populate_config_caps_final.sql

PROMPT
PROMPT Step 4: Populating CCL schema tables...
@08_populate_config_ccl.sql

PROMPT
PROMPT Step 5: Populating SWI schema tables...
@09_populate_config_swi.sql

PROMPT
PROMPT Step 6: Populating missing tables (INV_SUM, INV_*_FACT, etc.)...
@11_populate_config_missing_tables.sql

PROMPT
PROMPT ============================================================================
PROMPT Configuration population complete!
PROMPT ============================================================================
PROMPT
PROMPT Summary:
SELECT 
    OWNER,
    COUNT(DISTINCT TABLE_NAME) AS TABLE_COUNT,
    COUNT(*) AS TOTAL_CONFIG_ENTRIES,
    COUNT(DISTINCT EMAIL_ADDRESS) AS ANALYST_COUNT
FROM TBL_MDC_VALIDATION_CONFIG
GROUP BY OWNER
ORDER BY OWNER;

PROMPT
PROMPT Next steps:
PROMPT 1. Review configuration data
PROMPT 2. Update placeholder email addresses with actual analyst emails
PROMPT 3. Verify thresholds and grouping columns are correct
PROMPT 4. Create validation package (Phase 2)
PROMPT ============================================================================

