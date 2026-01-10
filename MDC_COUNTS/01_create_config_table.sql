-- ============================================================================
-- MDC Validation System - Configuration Table
-- ============================================================================
-- Purpose: Stores configuration for all tables to be validated, including
--          validation types, thresholds, grouping columns, and email assignments
-- ============================================================================

CREATE TABLE TBL_MDC_VALIDATION_CONFIG (
    CONFIG_ID              NUMBER PRIMARY KEY,
    OWNER                  VARCHAR2(10) NOT NULL,      -- CAPS, CCL, SWI, HR, PEI
    TABLE_NAME             VARCHAR2(128) NOT NULL,
    VALIDATION_TYPE        VARCHAR2(50) NOT NULL,     -- ROW_COUNT, COLUMN_COUNT, TABLE_COMPARE, etc.
    IS_ACTIVE              CHAR(1) DEFAULT 'Y' NOT NULL, -- Y/N
    GROUP_BY_COLUMNS       VARCHAR2(500),                -- Comma-separated: metric_type,cd_program,cd_svc_type
    COMPARE_TO_TABLE       VARCHAR2(128),               -- For table comparisons: e.g., FAD_SUM compares to FAD_FACT
    THRESHOLD_PCT          NUMBER DEFAULT 10,          -- Default 10, override per table (e.g., FCL tables use 2-3%)
    EMAIL_ADDRESS          VARCHAR2(255),              -- Placeholder: 'analyst1@example.com', 'analyst2@example.com'
    NOTES                  VARCHAR2(4000),             -- Validation rules, special handling notes
    PRIORITY               VARCHAR2(20) DEFAULT 'MEDIUM', -- HIGH, MEDIUM, LOW
    TIME_LOAD_RANGE_START  NUMBER,                     -- Default: CurrentTL-2, override if needed
    TIME_LOAD_RANGE_END    NUMBER,                     -- Default: CurrentTL
    CREATED_DATE           DATE DEFAULT SYSDATE,
    UPDATED_DATE           DATE,
    CONSTRAINT CHK_CONFIG_IS_ACTIVE CHECK (IS_ACTIVE IN ('Y', 'N')),
    CONSTRAINT CHK_CONFIG_PRIORITY CHECK (PRIORITY IN ('HIGH', 'MEDIUM', 'LOW')),
    CONSTRAINT CHK_CONFIG_THRESHOLD CHECK (THRESHOLD_PCT >= 0 AND THRESHOLD_PCT <= 100)
);

-- Create index for common queries
CREATE INDEX IDX_CONFIG_OWNER_TABLE ON TBL_MDC_VALIDATION_CONFIG(OWNER, TABLE_NAME);
CREATE INDEX IDX_CONFIG_ACTIVE ON TBL_MDC_VALIDATION_CONFIG(IS_ACTIVE, VALIDATION_TYPE);
CREATE INDEX IDX_CONFIG_EMAIL ON TBL_MDC_VALIDATION_CONFIG(EMAIL_ADDRESS);

-- Add comments to table and columns
COMMENT ON TABLE TBL_MDC_VALIDATION_CONFIG IS 'Configuration table for MDC validation system. Defines which tables to validate, how to validate them, and who receives the results.';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.CONFIG_ID IS 'Primary key, unique identifier for each configuration entry';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.OWNER IS 'Schema owner: CAPS, CCL, SWI, HR, or PEI';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.TABLE_NAME IS 'Name of the table to validate';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.VALIDATION_TYPE IS 'Type of validation: ROW_COUNT, COLUMN_COUNT, TABLE_COMPARE, etc.';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.IS_ACTIVE IS 'Y = active validation, N = inactive (skip this validation)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.GROUP_BY_COLUMNS IS 'Comma-separated list of columns to group by (e.g., metric_type,cd_program)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.COMPARE_TO_TABLE IS 'Table name to compare against (for TABLE_COMPARE validation type)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.THRESHOLD_PCT IS 'Percentage change threshold. Default 10%. Special cases: FCL tables use 2-3%';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.EMAIL_ADDRESS IS 'Email address of analyst assigned to review this table''s validation results';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.NOTES IS 'Special validation rules, handling notes, or business logic documentation';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.PRIORITY IS 'Priority level: HIGH, MEDIUM, or LOW';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.TIME_LOAD_RANGE_START IS 'Starting time load for validation (default: CurrentTL-2)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_CONFIG.TIME_LOAD_RANGE_END IS 'Ending time load for validation (default: CurrentTL)';

