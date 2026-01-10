-- ============================================================================
-- MDC Validation System - Populate Conditional Table Comparisons
-- ============================================================================
-- Purpose: Update table comparisons to support WHERE clause filters
-- ============================================================================

-- Update SVC_AOC_FACT to include WHERE clause filter
-- SVC_AOC_FACT should match SVC_SUM where CD_SVC_TYPE = 'AOC'
UPDATE TBL_MDC_VALIDATION_CONFIG
SET COMPARE_WHERE_CLAUSE = 'cd_svc_type = ''AOC'''
WHERE OWNER = 'CAPS' 
  AND TABLE_NAME = 'SVC_AOC_FACT'
  AND VALIDATION_TYPE = 'ROW_COUNT'
  AND COMPARE_TO_TABLE = 'SVC_SUM';

-- Update CCL facility comparison to use dt_load_process grouping
-- CCL_FACILITY_SUM vs FACILITY_FACT should compare by both id_time_load AND dt_load_process
UPDATE TBL_MDC_VALIDATION_CONFIG
SET COMPARE_GROUP_BY = 'dt_load_process'
WHERE OWNER = 'CCL'
  AND TABLE_NAME IN ('FACILITY_FACT', 'CCL_FACILITY_SUM')
  AND VALIDATION_TYPE = 'ROW_COUNT'
  AND COMPARE_TO_TABLE IS NOT NULL;

COMMIT;

PROMPT Conditional table comparisons updated successfully.

