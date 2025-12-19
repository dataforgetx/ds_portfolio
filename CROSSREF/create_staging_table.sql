-----------------------------------------------------------------------
-- Create Staging Table for ETL Files
-- This table stores all SQL and shell script contents from /usr/app/prodware
-----------------------------------------------------------------------

-- Drop table if exists (for re-creation)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE a$ETL_Files_Staging';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

-- Create staging table
CREATE TABLE a$ETL_Files_Staging (
    filename        VARCHAR2(200) NOT NULL,
    file_path       VARCHAR2(500) NOT NULL,
    line_num        NUMBER NOT NULL,
    line_content    VARCHAR2(4000),
    file_type       VARCHAR2(10) NOT NULL,  -- 'SQL' or 'SH'
    load_timestamp  DATE DEFAULT SYSDATE,
    CONSTRAINT pk_etl_staging PRIMARY KEY (file_path, line_num)
)
TABLESPACE RAM_BASE_D;

-- Create index on uppercased line content for fast searches
CREATE INDEX idx_etl_line_content_upper ON a$ETL_Files_Staging (UPPER(line_content))
TABLESPACE RAM_BASE_D;

-- Create index on file_path for grouping results
CREATE INDEX idx_etl_file_path ON a$ETL_Files_Staging (file_path)
TABLESPACE RAM_BASE_D;

-- Create composite index for CrossRef query pattern
CREATE INDEX idx_etl_search ON a$ETL_Files_Staging (UPPER(line_content), file_path, line_num)
TABLESPACE RAM_BASE_D;

-- Add comments
COMMENT ON TABLE a$ETL_Files_Staging IS 'Staging table for ETL script contents from /usr/app/prodware';
COMMENT ON COLUMN a$ETL_Files_Staging.filename IS 'Base filename (e.g., build_fact_sales.sql)';
COMMENT ON COLUMN a$ETL_Files_Staging.file_path IS 'Full file path (e.g., /usr/app/prodware/facts/build_fact_sales.sql)';
COMMENT ON COLUMN a$ETL_Files_Staging.line_num IS 'Line number in the file';
COMMENT ON COLUMN a$ETL_Files_Staging.line_content IS 'Content of the line (truncated to 4000 chars)';
COMMENT ON COLUMN a$ETL_Files_Staging.file_type IS 'File type: SQL or SH';
COMMENT ON COLUMN a$ETL_Files_Staging.load_timestamp IS 'When this row was loaded into the table';

-- Grant permissions (adjust schema/user as needed)
-- GRANT SELECT ON a$ETL_Files_Staging TO <your_user>;

SELECT 'Staging table a$ETL_Files_Staging created successfully' AS status FROM dual;

