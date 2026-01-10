-- ============================================================================
-- MDC Validation System - Populate FCL Distinct Count Validations
-- ============================================================================
-- Purpose: Add DISTINCT_COUNT validation configurations for FCL tables
-- Based on FCL_Table_Counts_Shawn_Adhoc.sql
-- ============================================================================

-- FCL_PMC_CHILD - Distinct person count
INSERT INTO TBL_MDC_VALIDATION_CONFIG (CONFIG_ID, OWNER, TABLE_NAME, VALIDATION_TYPE, IS_ACTIVE, DISTINCT_COUNT_COLUMN, THRESHOLD_PCT, EMAIL_ADDRESS, NOTES, PRIORITY)
VALUES (SEQ_MDC_CONFIG_ID.NEXTVAL, 'CAPS', 'FCL_PMC_CHILD', 'DISTINCT_COUNT', 'Y', 'id_pp_person', 2, 'analyst2@example.com', 'Distinct person count - should stay fairly stable month to month. Change >2% may need investigation.', 'HIGH');

-- FCL_PMC_CHILD - Distinct persons who exited
INSERT INTO TBL_MDC_VALIDATION_CONFIG (CONFIG_ID, OWNER, TABLE_NAME, VALIDATION_TYPE, IS_ACTIVE, DISTINCT_COUNT_COLUMN, DISTINCT_COUNT_CONDITION, THRESHOLD_PCT, EMAIL_ADDRESS, NOTES, PRIORITY)
VALUES (SEQ_MDC_CONFIG_ID.NEXTVAL, 'CAPS', 'FCL_PMC_CHILD', 'DISTINCT_COUNT', 'Y', 'id_pp_person', 'case when dt_exit is not null then id_pp_person end', 2, 'analyst2@example.com', 'Distinct persons who exited in the month. Exits bounce around but there will always be SOME exits.', 'MEDIUM');

-- FCL_PMC_CHILD_PLCMT - Distinct person count
INSERT INTO TBL_MDC_VALIDATION_CONFIG (CONFIG_ID, OWNER, TABLE_NAME, VALIDATION_TYPE, IS_ACTIVE, DISTINCT_COUNT_COLUMN, THRESHOLD_PCT, EMAIL_ADDRESS, NOTES, PRIORITY)
VALUES (SEQ_MDC_CONFIG_ID.NEXTVAL, 'CAPS', 'FCL_PMC_CHILD_PLCMT', 'DISTINCT_COUNT', 'Y', 'id_pp_person', 3, 'analyst2@example.com', 'Distinct person count - should match fcl_pmc_child count. Change of 3% up or down may need research.', 'HIGH');

-- FCL_PMC_CHILD_PLCMT - Distinct placement event count
INSERT INTO TBL_MDC_VALIDATION_CONFIG (CONFIG_ID, OWNER, TABLE_NAME, VALIDATION_TYPE, IS_ACTIVE, DISTINCT_COUNT_COLUMN, THRESHOLD_PCT, EMAIL_ADDRESS, NOTES, PRIORITY)
VALUES (SEQ_MDC_CONFIG_ID.NEXTVAL, 'CAPS', 'FCL_PMC_CHILD_PLCMT', 'DISTINCT_COUNT', 'Y', 'ID_SA_PLCMT_EVENT', 3, 'analyst2@example.com', 'Distinct placement event count. Change of 3% up or down may need research.', 'MEDIUM');

-- FCL_13_TMC_CHILD_PLCMT - Distinct person count
INSERT INTO TBL_MDC_VALIDATION_CONFIG (CONFIG_ID, OWNER, TABLE_NAME, VALIDATION_TYPE, IS_ACTIVE, DISTINCT_COUNT_COLUMN, THRESHOLD_PCT, EMAIL_ADDRESS, NOTES, PRIORITY)
VALUES (SEQ_MDC_CONFIG_ID.NEXTVAL, 'CAPS', 'FCL_13_TMC_CHILD_PLCMT', 'DISTINCT_COUNT', 'Y', 'id_pp_person', 3, 'analyst2@example.com', 'Distinct person count - should match fcl_pmc_child count. Change of 3% up or down may need research.', 'HIGH');

-- FCL_13_TMC_CHILD_PLCMT - Distinct placement event count
INSERT INTO TBL_MDC_VALIDATION_CONFIG (CONFIG_ID, OWNER, TABLE_NAME, VALIDATION_TYPE, IS_ACTIVE, DISTINCT_COUNT_COLUMN, THRESHOLD_PCT, EMAIL_ADDRESS, NOTES, PRIORITY)
VALUES (SEQ_MDC_CONFIG_ID.NEXTVAL, 'CAPS', 'FCL_13_TMC_CHILD_PLCMT', 'DISTINCT_COUNT', 'Y', 'ID_SA_PLCMT_EVENT', 3, 'analyst2@example.com', 'Distinct placement event count. Change of 3% up or down may need research.', 'MEDIUM');

COMMIT;

PROMPT FCL distinct count validations added successfully.

