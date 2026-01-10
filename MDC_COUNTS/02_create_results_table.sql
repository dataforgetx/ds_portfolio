-- ============================================================================
-- MDC Validation System - Results Table
-- ============================================================================
-- Purpose: Stores validation results for historical tracking, email generation,
--          and audit trail
-- ============================================================================

CREATE TABLE TBL_MDC_VALIDATION_RESULTS (
    RESULT_ID          NUMBER PRIMARY KEY,
    RUN_DATE           DATE DEFAULT SYSDATE NOT NULL,
    OWNER              VARCHAR2(10) NOT NULL,
    TABLE_NAME         VARCHAR2(128) NOT NULL,
    VALIDATION_TYPE    VARCHAR2(50) NOT NULL,
    TIME_LOAD         NUMBER,
    COLUMN_NAME        VARCHAR2(128),                 -- Nullable, for column counts
    GROUP_BY_VALUE     VARCHAR2(500),                  -- For grouped validations (e.g., metric_type='ABC')
    COUNT_VALUE        NUMBER,
    PRIOR_COUNT        NUMBER,                         -- For % change calculations
    PCT_CHANGE         NUMBER,                          -- Percentage change from prior time load
    STATUS             VARCHAR2(20),                   -- PASS, WARNING, ERROR
    MESSAGE            VARCHAR2(4000),                 -- Detailed message or error description
    EMAIL_ADDRESS      VARCHAR2(255),                  -- From config, for email grouping
    SEVERITY           VARCHAR2(20),                   -- HIGH, MEDIUM, LOW
    COMPARE_TABLE      VARCHAR2(128),                  -- For table comparisons
    COMPARE_COUNT      NUMBER,                         -- For table comparisons
    MATCH_STATUS       VARCHAR2(50),                  -- MATCH, NO_MATCH, DOES_NOT_EXIST
    CONSTRAINT CHK_RESULTS_STATUS CHECK (STATUS IN ('PASS', 'WARNING', 'ERROR', NULL)),
    CONSTRAINT CHK_RESULTS_SEVERITY CHECK (SEVERITY IN ('HIGH', 'MEDIUM', 'LOW', NULL)),
    CONSTRAINT CHK_RESULTS_PCT_CHANGE CHECK (PCT_CHANGE IS NULL OR (PCT_CHANGE >= -100 AND PCT_CHANGE <= 999999))
);

-- Create indexes for common queries
CREATE INDEX IDX_RESULTS_RUN_DATE ON TBL_MDC_VALIDATION_RESULTS(RUN_DATE);
CREATE INDEX IDX_RESULTS_EMAIL ON TBL_MDC_VALIDATION_RESULTS(EMAIL_ADDRESS, RUN_DATE);
CREATE INDEX IDX_RESULTS_STATUS ON TBL_MDC_VALIDATION_RESULTS(STATUS, SEVERITY, RUN_DATE);
CREATE INDEX IDX_RESULTS_TABLE ON TBL_MDC_VALIDATION_RESULTS(OWNER, TABLE_NAME, RUN_DATE);
CREATE INDEX IDX_RESULTS_TIME_LOAD ON TBL_MDC_VALIDATION_RESULTS(TIME_LOAD, RUN_DATE);

-- Add comments to table and columns
COMMENT ON TABLE TBL_MDC_VALIDATION_RESULTS IS 'Stores validation results for all tables. Used for email generation, historical tracking, and audit trail.';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.RESULT_ID IS 'Primary key, unique identifier for each result';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.RUN_DATE IS 'Date and time when validation was executed';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.OWNER IS 'Schema owner: CAPS, CCL, SWI, HR, or PEI';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.TABLE_NAME IS 'Name of the table that was validated';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.VALIDATION_TYPE IS 'Type of validation: ROW_COUNT, COLUMN_COUNT, TABLE_COMPARE, etc.';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.TIME_LOAD IS 'Time load identifier for this validation result';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.COLUMN_NAME IS 'Column name (for COLUMN_COUNT validation type, NULL for row counts)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.GROUP_BY_VALUE IS 'Grouping value(s) for grouped validations (e.g., metric_type=ABC,cd_program=XYZ)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.COUNT_VALUE IS 'Current count value for this validation';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.PRIOR_COUNT IS 'Previous time load count value (for % change calculation)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.PCT_CHANGE IS 'Percentage change from prior time load: ((current - prior) / prior) * 100';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.STATUS IS 'Validation status: PASS, WARNING, or ERROR';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.MESSAGE IS 'Detailed message, error description, or validation notes';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.EMAIL_ADDRESS IS 'Email address of analyst assigned to review this result (from config)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.SEVERITY IS 'Severity level: HIGH, MEDIUM, or LOW';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.COMPARE_TABLE IS 'Table name being compared against (for TABLE_COMPARE validation type)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.COMPARE_COUNT IS 'Count value from comparison table';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.MATCH_STATUS IS 'Match status for table comparisons: MATCH, NO_MATCH, or DOES_NOT_EXIST';

