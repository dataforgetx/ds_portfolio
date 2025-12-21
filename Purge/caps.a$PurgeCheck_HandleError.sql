------------------------------------------------------------------------------------------
-- Filename: caps.a$PurgeCheck_HandleError.sql
--
-- Purpose: Error handler procedure called by a$PurgeCheck when purge errors are detected.
--          Logs errors to the queue table for processing by automated fix procedures.
--
------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE CAPS.a$PurgeCheck_HandleError(
    p_table_name        IN VARCHAR2,
    p_error_count       IN NUMBER,
    p_category          IN VARCHAR2,
    p_definition_table  IN VARCHAR2 DEFAULT NULL
) IS
-----------------------------------------------------------------------------
-- Parameters:
--   p_table_name       - Destination table with purge errors (e.g., inv_princ_sum)
--   p_error_count      - Number of records not marked as purged
--   p_category         - Category: 'CASE', 'PERSON', or 'STAGE'
--   p_definition_table - Source definition table (prg_caps_case, prg_person, etc.)
--
-- Purpose:
--   Logs purge check errors to the queue table for automated processing.
--   Prevents duplicate entries for the same table within a short time window.
-----------------------------------------------------------------------------

    v_existing_id       NUMBER;
    v_existing_status   VARCHAR2(20);
    v_duplicate_window  NUMBER := 1/24; -- 1 hour window to consider duplicates
    
BEGIN
    
    -- Validate inputs
    IF p_table_name IS NULL OR p_error_count IS NULL OR p_category IS NULL THEN
        post_a$log('PurgeCheck_HandleError', 'ERROR: Required parameters are NULL');
        RETURN;
    END IF;
    
    IF p_error_count <= 0 THEN
        -- No error to log
        RETURN;
    END IF;
    
    -- Validate category
    IF p_category NOT IN ('CASE', 'PERSON', 'STAGE') THEN
        post_a$log('PurgeCheck_HandleError', 'ERROR: Invalid category: '||p_category);
        RETURN;
    END IF;
    
    -- Check if there's already a pending or processing entry for this table
    -- within the duplicate window (to avoid duplicate entries from multiple runs)
    BEGIN
        SELECT id, status
        INTO v_existing_id, v_existing_status
        FROM CAPS.a$purgecheck_errors_queue
        WHERE table_name = UPPER(TRIM(p_table_name))
          AND category = p_category
          AND detected_date >= SYSDATE - v_duplicate_window
          AND status IN ('PENDING', 'PROCESSING')
        ORDER BY detected_date DESC
        FETCH FIRST 1 ROW ONLY;
        
        -- If found, update the error count if it's different
        IF v_existing_id IS NOT NULL THEN
            IF v_existing_status = 'PENDING' THEN
                UPDATE CAPS.a$purgecheck_errors_queue
                SET error_count = p_error_count,
                    definition_table = NVL(p_definition_table, definition_table),
                    notes = 'Error count updated: '||TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')
                WHERE id = v_existing_id;
                
                post_a$log('PurgeCheck_HandleError', 
                    'Updated existing queue entry (ID='||v_existing_id||') for '||p_table_name||
                    ' with new error count: '||p_error_count);
                COMMIT;
            ELSE
                -- Entry is currently being processed, just log
                post_a$log('PurgeCheck_HandleError', 
                    'Queue entry (ID='||v_existing_id||') for '||p_table_name||
                    ' is already being processed (status='||v_existing_status||')');
            END IF;
            RETURN;
        END IF;
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- No existing entry, proceed to insert new one
            NULL;
        WHEN OTHERS THEN
            post_a$log('PurgeCheck_HandleError', 
                'ERROR checking for existing entry: '||SQLERRM);
            -- Continue to insert anyway
    END;
    
    -- Clean up old queue entries (older than 90 days) before inserting new ones
    -- for queue table to avoid unbounded growth
    BEGIN
        DELETE FROM CAPS.a$purgecheck_errors_queue
        WHERE updated_date < SYSDATE - 90;
        
        IF SQL%ROWCOUNT > 0 THEN
            post_a$log('PurgeCheck_HandleError', 
                'Cleaned up '||SQL%ROWCOUNT||' old queue entry/entries (older than 90 days)');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- Don't fail if cleanup fails, just log warning
            post_a$log('PurgeCheck_HandleError', 
                'WARNING: Error cleaning up old queue entries: '||SQLERRM||' - continuing');
    END;
    
    -- Insert new error into queue
    BEGIN
        INSERT INTO CAPS.a$purgecheck_errors_queue (
            table_name,
            error_count,
            category,
            definition_table,
            status,
            detected_date
        ) VALUES (
            UPPER(TRIM(p_table_name)),
            p_error_count,
            p_category,
            p_definition_table,
            'PENDING',
            SYSDATE
        );
        
        COMMIT;
        
        post_a$log('PurgeCheck_HandleError', 
            'Logged error to queue: '||p_table_name||
            ' ('||p_category||') - '||p_error_count||' records');
            
    EXCEPTION
        WHEN OTHERS THEN
            post_a$log('PurgeCheck_HandleError', 
                'ERROR inserting into queue: '||SQLERRM||
                ' for table: '||p_table_name);
            ROLLBACK;
            RAISE;
    END;
    
END a$PurgeCheck_HandleError;
/

-- Add comment
COMMENT ON PROCEDURE CAPS.a$PurgeCheck_HandleError IS 
    'Error handler called by a$PurgeCheck when purge errors are detected. Logs errors to queue table for automated processing.';

-- Grant execute permission (adjust as needed)
-- GRANT EXECUTE ON CAPS.a$PurgeCheck_HandleError TO <user>;

PROMPT Procedure CAPS.a$PurgeCheck_HandleError created successfully

