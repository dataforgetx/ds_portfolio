------------------------------------------------------------------------------------------
-- Filename: caps.a$PurgeCheck_ResetStuckEntries.sql
--
-- Purpose: Procedure to reset queue entries that are stuck in PROCESSING status.
--          This can happen if automation crashes mid-process or if manual intervention
--          is needed. Resets entries to PENDING so they can be reprocessed.
--
-- Usage:
--   1. AUTOMATIC: Called automatically by a$PurgeCheckAndFix at startup (Step 0)
--   2. MANUAL: Can be called manually when needed to recover stuck entries
--   3. SCHEDULED: Can be scheduled to run periodically (e.g., daily) via DBMS_SCHEDULER
--
------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE CAPS.a$PurgeCheck_ResetStuckEntries(
    p_hours_stuck          IN NUMBER DEFAULT 24,  -- Hours an entry must be stuck before reset
    p_reset_to_status      IN VARCHAR2 DEFAULT 'PENDING',  -- Status to reset to
    p_dry_run              IN BOOLEAN DEFAULT FALSE,
    p_reset_count          OUT NUMBER,
    p_reset_list           OUT VARCHAR2
) IS
-----------------------------------------------------------------------------
-- Parameters:
--   p_hours_stuck          - Hours an entry must be in PROCESSING before considered stuck
--   p_reset_to_status      - Status to reset entries to (default 'PENDING')
--   p_dry_run              - If TRUE, only reports what would be reset, doesn't actually reset
--   p_reset_count          - Output: Number of entries reset
--   p_reset_list           - Output: List of entries that were reset
--
-- Purpose:
--   Finds queue entries stuck in PROCESSING status for more than specified hours
--   and resets them to PENDING (or specified status) so they can be reprocessed.
--
-- Usage:
--   DECLARE
--     v_count NUMBER;
--     v_list VARCHAR2(4000);
--   BEGIN
--     CAPS.a$PurgeCheck_ResetStuckEntries(
--       p_hours_stuck => 24,
--       p_reset_to_status => 'PENDING',
--       p_dry_run => FALSE,
--       p_reset_count => v_count,
--       p_reset_list => v_list
--     );
--     DBMS_OUTPUT.PUT_LINE('Reset '||v_count||' entries');
--     DBMS_OUTPUT.PUT_LINE(v_list);
--   END;
--   /
-----------------------------------------------------------------------------

    v_stuck_threshold      DATE;
    v_entry_count          NUMBER := 0;
    v_entry_list           VARCHAR2(4000) := '';
    v_episode_status       VARCHAR2(20);
    v_episode_close        DATE;
    
    -- Cursor for stuck entries
    CURSOR c_stuck_entries IS
        SELECT id,
               table_name,
               episode_id,
               updated_date,
               ROUND((SYSDATE - updated_date) * 24, 2) AS hours_stuck
        FROM CAPS.a$purgecheck_errors_queue
        WHERE status = 'PROCESSING'
          AND updated_date < SYSDATE - (p_hours_stuck / 24)
        ORDER BY updated_date ASC;
    
BEGIN
    
    -- Initialize outputs
    p_reset_count := 0;
    p_reset_list := '';
    
    -- Validate reset status
    IF p_reset_to_status NOT IN ('PENDING', 'FAILED', 'MANUAL') THEN
        RAISE_APPLICATION_ERROR(-20001, 
            'Invalid reset status: '||p_reset_to_status||
            '. Must be PENDING, FAILED, or MANUAL');
    END IF;
    
    -- Calculate threshold
    v_stuck_threshold := SYSDATE - (p_hours_stuck / 24);
    
    post_a$log('PurgeCheck_ResetStuckEntries', 
        'Starting reset of stuck PROCESSING entries');
    post_a$log('PurgeCheck_ResetStuckEntries', 
        'Threshold: Entries stuck for more than '||p_hours_stuck||' hours');
    post_a$log('PurgeCheck_ResetStuckEntries', 
        'Reset to status: '||p_reset_to_status);
    post_a$log('PurgeCheck_ResetStuckEntries', 
        'Dry run: '||CASE WHEN p_dry_run THEN 'YES' ELSE 'NO' END);
    
    -- Process each stuck entry
    FOR rec IN c_stuck_entries LOOP
        v_entry_count := v_entry_count + 1;
        
        -- Check episode status to provide context
        BEGIN
            SELECT episode_status, episode_close
            INTO v_episode_status, v_episode_close
            FROM ram.a$rectifydb_log_master
            WHERE id = rec.episode_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_episode_status := 'NOT FOUND';
                v_episode_close := NULL;
            WHEN OTHERS THEN
                v_episode_status := 'ERROR: '||SQLERRM;
                v_episode_close := NULL;
        END;
        
        -- Build list of entries
        IF v_entry_list IS NOT NULL AND LENGTH(v_entry_list) > 0 THEN
            v_entry_list := v_entry_list||chr(10);
        END IF;
        v_entry_list := v_entry_list||
            'ID='||rec.id||
            ', Table='||rec.table_name||
            ', Episode='||NVL(TO_CHAR(rec.episode_id), 'NULL')||
            ', Stuck='||rec.hours_stuck||' hours'||
            ', Episode Status='||v_episode_status;
        
        -- Reset entry if not dry run
        IF NOT p_dry_run THEN
            BEGIN
                UPDATE CAPS.a$purgecheck_errors_queue
                SET status = p_reset_to_status,
                    error_message = CASE 
                        WHEN p_reset_to_status = 'PENDING' THEN 
                            'Reset from PROCESSING - was stuck for '||rec.hours_stuck||' hours. '||
                            'Episode status: '||v_episode_status||'. '||
                            'Original error message: '||NVL(error_message, 'N/A')
                        ELSE 
                            error_message 
                    END,
                    notes = NVL(notes, '')||chr(10)||
                        'Reset on '||TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')||
                        ' - was stuck in PROCESSING for '||rec.hours_stuck||' hours',
                    updated_date = SYSDATE,
                    updated_by = USER
                WHERE id = rec.id;
                
                post_a$log('PurgeCheck_ResetStuckEntries', 
                    'Reset entry ID='||rec.id||
                    ' (Table='||rec.table_name||
                    ', Episode='||NVL(TO_CHAR(rec.episode_id), 'NULL')||
                    ', Stuck='||rec.hours_stuck||' hours) to status '||p_reset_to_status);
                
            EXCEPTION
                WHEN OTHERS THEN
                    post_a$log('PurgeCheck_ResetStuckEntries', 
                        'ERROR resetting entry ID='||rec.id||': '||SQLERRM);
                    -- Continue with other entries
            END;
        ELSE
            -- Dry run - just log
            post_a$log('PurgeCheck_ResetStuckEntries', 
                'DRY RUN: Would reset entry ID='||rec.id||
                ' (Table='||rec.table_name||
                ', Episode='||NVL(TO_CHAR(rec.episode_id), 'NULL')||
                ', Stuck='||rec.hours_stuck||' hours) to status '||p_reset_to_status);
        END IF;
    END LOOP;
    
    -- Commit if not dry run
    IF NOT p_dry_run AND v_entry_count > 0 THEN
        COMMIT;
        post_a$log('PurgeCheck_ResetStuckEntries', 
            'Committed reset of '||v_entry_count||' entry/entries');
    END IF;
    
    -- Set outputs
    p_reset_count := v_entry_count;
    p_reset_list := v_entry_list;
    
    -- Final log
    IF v_entry_count = 0 THEN
        post_a$log('PurgeCheck_ResetStuckEntries', 
            'No stuck entries found (threshold: '||p_hours_stuck||' hours)');
    ELSE
        IF p_dry_run THEN
            post_a$log('PurgeCheck_ResetStuckEntries', 
                'DRY RUN: Found '||v_entry_count||' stuck entry/entries that would be reset');
        ELSE
            post_a$log('PurgeCheck_ResetStuckEntries', 
                'Successfully reset '||v_entry_count||' stuck entry/entries');
        END IF;
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        post_a$log('PurgeCheck_ResetStuckEntries', 
            'FATAL ERROR: '||SQLERRM);
        ROLLBACK;
        RAISE;
        
END a$PurgeCheck_ResetStuckEntries;
/

-- Add comment
COMMENT ON PROCEDURE CAPS.a$PurgeCheck_ResetStuckEntries IS 
    'Resets queue entries stuck in PROCESSING status. Useful for recovery after automation crashes or manual intervention.';

-- Grant execute permission (adjust as needed)
-- GRANT EXECUTE ON CAPS.a$PurgeCheck_ResetStuckEntries TO <user>;

PROMPT Procedure CAPS.a$PurgeCheck_ResetStuckEntries created successfully

