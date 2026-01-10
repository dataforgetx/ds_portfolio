-- ============================================================================
-- Add 12-Month Average Columns to Results Table
-- ============================================================================
-- Purpose: Support column count validation with 12-month average comparison
-- ============================================================================

ALTER TABLE TBL_MDC_VALIDATION_RESULTS
ADD (
    AVG_12MONTH_COUNT      NUMBER,                         -- 12-month average count
    PCT_CHANGE_12MONTH     NUMBER                          -- % change vs 12-month average
);

COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.AVG_12MONTH_COUNT IS '12-month rolling average count (for column count validations)';
COMMENT ON COLUMN TBL_MDC_VALIDATION_RESULTS.PCT_CHANGE_12MONTH IS 'Percentage change from 12-month average: ((current - avg_12month) / avg_12month) * 100';

-- Update constraint to include new PCT_CHANGE_12MONTH
ALTER TABLE TBL_MDC_VALIDATION_RESULTS
DROP CONSTRAINT CHK_RESULTS_PCT_CHANGE;

ALTER TABLE TBL_MDC_VALIDATION_RESULTS
ADD CONSTRAINT CHK_RESULTS_PCT_CHANGE CHECK (
    (PCT_CHANGE IS NULL OR (PCT_CHANGE >= -100 AND PCT_CHANGE <= 999999))
    AND (PCT_CHANGE_12MONTH IS NULL OR (PCT_CHANGE_12MONTH >= -100 AND PCT_CHANGE_12MONTH <= 999999))
);

