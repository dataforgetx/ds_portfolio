------------------------------------------------------------------------------------------
-- Filename: caps.a$purgecheck_errors_queue.sql
--
-- Purpose: Queue table to track purge check errors and their resolution status
--          Used by automated purge data fix process
--
------------------------------------------------------------------------------------------

-- Drop existing objects if they exist (for re-creation)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE CAPS.a$purgecheck_errors_queue';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN  -- Table does not exist
         RAISE;
      END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE CAPS.seq_purgecheck_errors_queue';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -2289 THEN  -- Sequence does not exist
         RAISE;
      END IF;
END;
/

-- Create sequence for primary key
CREATE SEQUENCE CAPS.seq_purgecheck_errors_queue
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;

-- Create queue table
CREATE TABLE CAPS.a$purgecheck_errors_queue (
    id                      NUMBER PRIMARY KEY,
    table_name              VARCHAR2(60) NOT NULL,
    error_count             NUMBER NOT NULL,
    category                VARCHAR2(10) NOT NULL,  -- 'CASE', 'PERSON', 'STAGE'
    definition_table        VARCHAR2(60),           -- Source definition table (prg_caps_case, prg_person, etc.)
    detected_date           DATE DEFAULT SYSDATE NOT NULL,
    status                  VARCHAR2(20) DEFAULT 'PENDING' NOT NULL,
    episode_id              NUMBER,                 -- Episode ID from mrs.open
    processed_date          DATE,                   -- When processing completed/failed
    error_message           VARCHAR2(4000),         -- Error details if processing failed
    retry_count             NUMBER DEFAULT 0,
    created_by              VARCHAR2(30) DEFAULT USER,
    updated_date             DATE DEFAULT SYSDATE,
    updated_by              VARCHAR2(30) DEFAULT USER,
    datafix_filename        VARCHAR2(200),           -- Name of datafix file created
    notes                   VARCHAR2(4000),          -- Additional notes/comments
    
    CONSTRAINT chk_category CHECK (category IN ('CASE', 'PERSON', 'STAGE')),
    CONSTRAINT chk_status CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'SKIPPED', 'MANUAL')),
    CONSTRAINT chk_error_count CHECK (error_count >= 0),
    CONSTRAINT chk_retry_count CHECK (retry_count >= 0)
);

-- Create indexes for performance
CREATE INDEX idx_purgecheck_queue_status ON CAPS.a$purgecheck_errors_queue(status);
CREATE INDEX idx_purgecheck_queue_table ON CAPS.a$purgecheck_errors_queue(table_name);
CREATE INDEX idx_purgecheck_queue_detected ON CAPS.a$purgecheck_errors_queue(detected_date);
CREATE INDEX idx_purgecheck_queue_episode ON CAPS.a$purgecheck_errors_queue(episode_id);

-- Create composite index for common queries (status + detected_date)
CREATE INDEX idx_purgecheck_queue_status_date ON CAPS.a$purgecheck_errors_queue(status, detected_date);

-- Create trigger to auto-populate ID and handle updated_date
CREATE OR REPLACE TRIGGER trg_purgecheck_queue_id
   BEFORE INSERT ON CAPS.a$purgecheck_errors_queue
   FOR EACH ROW
BEGIN
   IF :NEW.id IS NULL THEN
      :NEW.id := CAPS.seq_purgecheck_errors_queue.NEXTVAL;
   END IF;
   :NEW.created_by := USER;
   :NEW.updated_by := USER;
   :NEW.updated_date := SYSDATE;
END;
/

-- Create trigger to update updated_date on updates
CREATE OR REPLACE TRIGGER trg_purgecheck_queue_update
   BEFORE UPDATE ON CAPS.a$purgecheck_errors_queue
   FOR EACH ROW
BEGIN
   :NEW.updated_by := USER;
   :NEW.updated_date := SYSDATE;
END;
/

-- Add comments for documentation
COMMENT ON TABLE CAPS.a$purgecheck_errors_queue IS 
   'Queue table to track purge check errors detected by a$PurgeCheck and their resolution status. Used by automated purge data fix process.';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.id IS 
   'Primary key, auto-generated from sequence';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.table_name IS 
   'Destination table name with purge errors (e.g., inv_princ_sum, afcars_cfsr_sum)';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.error_count IS 
   'Number of records not marked as purged in the destination table';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.category IS 
   'Category of purge error: CASE, PERSON, or STAGE';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.definition_table IS 
   'Source definition table (prg_caps_case, prg_person, prg_incoming_detail)';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.detected_date IS 
   'Date/time when the error was first detected by a$PurgeCheck';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.status IS 
   'Current status: PENDING (not yet processed), PROCESSING (currently being fixed), COMPLETED (successfully fixed), FAILED (fix failed), SKIPPED (manually skipped), MANUAL (requires manual intervention)';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.episode_id IS 
   'Episode ID from ram.a$rectifydb_log_master when mrs.open() was called';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.processed_date IS 
   'Date/time when processing completed (successfully or with failure)';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.error_message IS 
   'Error message if processing failed';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.retry_count IS 
   'Number of times processing has been attempted for this error';

COMMENT ON COLUMN CAPS.a$purgecheck_errors_queue.datafix_filename IS 
   'Name of the datafix file created for this error (e.g., MDC01_2025_PURGED_datafix_inv_princ_sum.sql)';

-- Grant necessary permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE ON CAPS.a$purgecheck_errors_queue TO <user>;
-- GRANT SELECT ON CAPS.seq_purgecheck_errors_queue TO <user>;

-- Create view for easy querying of pending errors
CREATE OR REPLACE VIEW CAPS.v_purgecheck_errors_pending AS
SELECT 
    id,
    table_name,
    error_count,
    category,
    definition_table,
    detected_date,
    retry_count,
    datafix_filename,
    notes,
    ROUND((SYSDATE - detected_date) * 24, 2) AS hours_since_detected
FROM CAPS.a$purgecheck_errors_queue
WHERE status = 'PENDING'
ORDER BY detected_date ASC;

COMMENT ON VIEW CAPS.v_purgecheck_errors_pending IS 
   'View showing all pending purge check errors awaiting processing';

-- Create view for summary statistics
CREATE OR REPLACE VIEW CAPS.v_purgecheck_errors_summary AS
SELECT 
    status,
    category,
    COUNT(*) AS error_count,
    SUM(error_count) AS total_records,
    MIN(detected_date) AS oldest_error,
    MAX(detected_date) AS newest_error,
    AVG(retry_count) AS avg_retries
FROM CAPS.a$purgecheck_errors_queue
GROUP BY status, category
ORDER BY status, category;

COMMENT ON VIEW CAPS.v_purgecheck_errors_summary IS 
   'Summary statistics of purge check errors by status and category';

-- Verification queries (commented out - uncomment to test)
/*
-- Check table structure
SELECT column_name, data_type, data_length, nullable, data_default
FROM user_tab_columns
WHERE table_name = 'A$PURGECHECK_ERRORS_QUEUE'
ORDER BY column_id;

-- Check indexes
SELECT index_name, column_name, column_position
FROM user_ind_columns
WHERE table_name = 'A$PURGECHECK_ERRORS_QUEUE'
ORDER BY index_name, column_position;

-- Test insert
INSERT INTO CAPS.a$purgecheck_errors_queue (
    table_name, 
    error_count, 
    category, 
    definition_table
) VALUES (
    'inv_princ_sum',
    2,
    'PERSON',
    'prg_person'
);
COMMIT;

-- Verify insert
SELECT * FROM CAPS.a$purgecheck_errors_queue;

-- Check pending view
SELECT * FROM CAPS.v_purgecheck_errors_pending;

-- Check summary view
SELECT * FROM CAPS.v_purgecheck_errors_summary;

-- Cleanup test data
DELETE FROM CAPS.a$purgecheck_errors_queue WHERE table_name = 'inv_princ_sum';
COMMIT;
*/

PROMPT Table CAPS.a$purgecheck_errors_queue created successfully
PROMPT Sequence CAPS.seq_purgecheck_errors_queue created successfully
PROMPT Views created: v_purgecheck_errors_pending, v_purgecheck_errors_summary

