-- ============================================================================
-- MDC Validation System - Sequences
-- ============================================================================
-- Purpose: Generate primary key values for configuration and results tables
-- ============================================================================

-- Sequence for TBL_MDC_VALIDATION_CONFIG
CREATE SEQUENCE SEQ_MDC_CONFIG_ID
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- Sequence for TBL_MDC_VALIDATION_RESULTS
CREATE SEQUENCE SEQ_MDC_RESULT_ID
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- Add comments
COMMENT ON SEQUENCE SEQ_MDC_CONFIG_ID IS 'Sequence for generating CONFIG_ID primary key values';
COMMENT ON SEQUENCE SEQ_MDC_RESULT_ID IS 'Sequence for generating RESULT_ID primary key values';

