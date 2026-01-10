-- ============================================================================
-- MDC Validation System - Add Missing Validation Type Support
-- ============================================================================
-- Purpose: Add columns to support new validation types:
--  1. DISTINCT_COUNT validations
--  2. Conditional table comparisons (WHERE clause filters)
--  3. dt_load_process grouping in comparisons
--  4. External source comparisons (@mdc_all)
--  5. Data validation checks
-- ============================================================================

-- Add new columns to config table
ALTER TABLE TBL_MDC_VALIDATION_CONFIG ADD (
    DISTINCT_COUNT_COLUMN      VARCHAR2(128),  -- Column name for DISTINCT_COUNT (e.g., 'id_pp_person')
    DISTINCT_COUNT_CONDITION   VARCHAR2(4000), -- Optional CASE WHEN condition for DISTINCT_COUNT (e.g., 'case when dt_exit is not null then id_pp_person end')
    COMPARE_WHERE_CLAUSE        VARCHAR2(4000), -- WHERE clause for conditional table comparisons (e.g., "cd_svc_type = 'AOC'")
    COMPARE_GROUP_BY            VARCHAR2(500),  -- Additional grouping for comparisons (e.g., 'dt_load_process' for CCL facility)
    COMPARE_TO_SOURCE          VARCHAR2(128),  -- Source database link for external comparisons (e.g., '@mdc_all', '@ware')
    DATA_VALIDATION_QUERY      CLOB            -- Full SQL query for data validation checks (MINUS operations)
);

-- Update constraint to include new validation types (if constraint exists)
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE TBL_MDC_VALIDATION_CONFIG DROP CONSTRAINT CHK_CONFIG_VAL_TYPE';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Constraint may not exist yet
END;

ALTER TABLE TBL_MDC_VALIDATION_CONFIG ADD CONSTRAINT CHK_CONFIG_VAL_TYPE CHECK (
    VALIDATION_TYPE IN (
        'ROW_COUNT', 
        'COLUMN_COUNT', 
        'TABLE_COMPARE', 
        'ROW_COUNT_GROUPED', 
        'ROW_COUNT_DT_LOAD',
        'DISTINCT_COUNT',
        'DATA_VALIDATION'
    )
);

-- Add comments for new columns
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.DISTINCT_COUNT_COLUMN IS 'Column name for DISTINCT_COUNT validation (e.g., id_pp_person for FCL tables)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.DISTINCT_COUNT_CONDITION IS 'Optional CASE WHEN condition for DISTINCT_COUNT (e.g., case when dt_exit is not null then id_pp_person end)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.COMPARE_WHERE_CLAUSE IS 'WHERE clause filter for conditional table comparisons (e.g., cd_svc_type = ''AOC'' for SVC_AOC_FACT)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.COMPARE_GROUP_BY IS 'Additional grouping column for table comparisons (e.g., dt_load_process for CCL facility comparison)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.COMPARE_TO_SOURCE IS 'Source database link for external comparisons (e.g., @mdc_all for MDC source comparisons)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.DATA_VALIDATION_QUERY IS 'Full SQL query for data validation checks using MINUS operations (for MDC_ALL comparisons)';

COMMIT;

PROMPT Configuration table updated with new validation type support columns.

