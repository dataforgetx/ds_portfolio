-- ============================================================================
-- Create Table to Store HTML Validation Reports
-- ============================================================================
-- Purpose: Store HTML validation reports in database table (no directory permissions needed)
-- ============================================================================

CREATE TABLE TBL_MDC_VALIDATION_HTML_REPORTS (
    REPORT_ID          NUMBER PRIMARY KEY,
    EMAIL_ADDRESS      VARCHAR2(255) NOT NULL,
    RUN_DATE           DATE NOT NULL,
    TABLE_COUNT        NUMBER,
    HTML_CONTENT       CLOB,
    CREATED_DATE       DATE DEFAULT SYSDATE,
    CONSTRAINT UK_REPORT_EMAIL_DATE UNIQUE (EMAIL_ADDRESS, RUN_DATE)
);

-- Create sequence for report IDs
CREATE SEQUENCE SEQ_MDC_REPORT_ID
    START WITH 1
    INCREMENT BY 1
    NOCACHE;

-- Create index for faster queries
CREATE INDEX IDX_MDC_HTML_REPORTS_EMAIL ON TBL_MDC_VALIDATION_HTML_REPORTS(EMAIL_ADDRESS);
CREATE INDEX IDX_MDC_HTML_REPORTS_DATE ON TBL_MDC_VALIDATION_HTML_REPORTS(RUN_DATE);

-- Add comments
COMMENT ON TABLE TBL_MDC_VALIDATION_HTML_REPORTS IS 'Stores HTML validation reports for each analyst';
COMMENT ON COLUMN TBL_MDC_VALIDATION_HTML_REPORTS.REPORT_ID IS 'Primary key';
COMMENT ON COLUMN TBL_MDC_VALIDATION_HTML_REPORTS.EMAIL_ADDRESS IS 'Analyst email address';
COMMENT ON COLUMN TBL_MDC_VALIDATION_HTML_REPORTS.RUN_DATE IS 'Validation run date';
COMMENT ON COLUMN TBL_MDC_VALIDATION_HTML_REPORTS.TABLE_COUNT IS 'Number of tables in this report';
COMMENT ON COLUMN TBL_MDC_VALIDATION_HTML_REPORTS.HTML_CONTENT IS 'HTML content of the validation report';
COMMENT ON COLUMN TBL_MDC_VALIDATION_HTML_REPORTS.CREATED_DATE IS 'When the report was created';

PROMPT Table TBL_MDC_VALIDATION_HTML_REPORTS created successfully
PROMPT

