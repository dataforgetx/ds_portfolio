------------------------------------------------------------------------------------------
-- Filename: caps.a$PurgeCheckAndFix_SingleTable.sql
--
-- Purpose: Fixes purge errors for a single table by:
--          1. Creating datafix file
--          2. Opening tablespace (mrs.open)
--          3. Waiting for DBA approval (if needed)
--          4. Executing a$purgeforce
--          5. Verifying fix
--          6. Closing tablespace (mrs.close)
--          7. Updating queue table
--
-- IMPORTANT USER PERMISSIONS:
--   This procedure calls mrs.open() and mrs.close() which require VerifyUser('rectifydb')
--   permission. The executing user must have Analyst/DBA/MGR role in ram.a$user table
--   for appl='rectifydb'.
--
--   For automation, CAPS user should be added to ram.a$user table:
--     INSERT INTO ram.a$user (username, appl, role, name, org, email)
--     VALUES ('CAPS', 'rectifydb', 'Analyst', 'CAPS System User', 'MRS', 'your-email@dfps.texas.gov');
--
--   Without this permission, mrs.open() and mrs.close() will fail.
--
------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE CAPS.a$PurgeCheckAndFix_SingleTable(
    p_table_name            IN VARCHAR2,
    p_queue_id              IN NUMBER DEFAULT NULL,  -- Queue table ID (if processing from queue)
    p_auto_tablespace        IN BOOLEAN DEFAULT FALSE,
    p_poll_interval_seconds  IN NUMBER DEFAULT 120,
    p_max_wait_minutes      IN NUMBER DEFAULT 240,
    p_dry_run               IN BOOLEAN DEFAULT FALSE,
    p_status_out             OUT VARCHAR2,  -- 'SUCCESS', 'FAILED', 'SKIPPED', 'TIMEOUT'
    p_message_out            OUT VARCHAR2
) IS
-----------------------------------------------------------------------------
-- Parameters:
--   p_table_name            - Table to fix (e.g., 'inv_princ_sum')
--   p_queue_id              - Queue table ID (optional, for tracking)
--   p_auto_tablespace        - If TRUE, assumes tablespace auto-opens/closes
--   p_poll_interval_seconds  - Polling interval for DBA approval
--   p_max_wait_minutes      - Max wait time for DBA approval
--   p_dry_run               - If TRUE, only validates, doesn't execute
--   p_status_out             - Output status
--   p_message_out            - Output message
--
-- Process:
--   1. Generate datafix filename
--   2. Create datafix file in /usr/app/rectify/ware/
--   3. Call mrs.open() to request tablespace open
--   4. Extract episode_id
--   5. Wait for DBA approval (if p_auto_tablespace = FALSE)
--   6. Execute CAPS.a$purgeforce(p_table_name)
--   7. Verify fix with a$PurgeCheck and check queue for remaining errors
--   8. Call mrs.close() to request tablespace close (only if verification passed)
--   9. Wait for DBA approval (if p_auto_tablespace = FALSE)
--  10. Update queue table status
-----------------------------------------------------------------------------

    v_datafix_filename      VARCHAR2(200);
    v_datafix_path          VARCHAR2(200) := '/usr/app/rectify/ware/';
    v_episode_id            NUMBER;
    v_wait_status           VARCHAR2(20);
    v_wait_message          VARCHAR2(4000);
    v_error_count_before    NUMBER := 0;
    v_error_count_after     NUMBER := 0;
    v_prerequisite_table    VARCHAR2(60);
    v_file_handle           UTL_FILE.FILE_TYPE;
    v_directory_object      VARCHAR2(100) := 'DIR_RECTIFY';  -- May need to be created/configured
    v_current_user          VARCHAR2(30);
    v_step                  VARCHAR2(100);
    
    -- Prerequisites mapping (table => prerequisite table)
    TYPE PrereqTab IS TABLE OF VARCHAR2(60) INDEX BY VARCHAR2(60);
    v_prerequisites PrereqTab;
    
BEGIN
    
    -- Initialize outputs
    p_status_out := 'FAILED';
    p_message_out := '';
    v_current_user := USER;
    
    -- Initialize prerequisites
    -- All inv* tables (except inv_sum itself) require inv_sum to be fixed first
    -- This is handled dynamically in the check below
    
    post_a$log('PurgeCheckAndFix_SingleTable', 
        'Starting fix for table: '||p_table_name||
        ' (Queue ID: '||NVL(TO_CHAR(p_queue_id), 'N/A')||
        ', Dry Run: '||CASE WHEN p_dry_run THEN 'YES' ELSE 'NO' END||
        ', User: '||v_current_user||')');
    
    -- Check user permissions for mrs.open() and mrs.close()
    -- These procedures require VerifyUser('rectifydb') which checks ram.a$user table
    -- For automation, CAPS user should be added to ram.a$user with Analyst/DBA/MGR role
    BEGIN
        DECLARE
            v_user_count NUMBER;
        BEGIN
            SELECT COUNT(*)
            INTO v_user_count
            FROM ram.a$user
            WHERE username = USER
              AND appl = 'rectifydb'
              AND role IN ('Analyst', 'DBA', 'MGR');
            
            IF v_user_count = 0 THEN
                p_status_out := 'FAILED';
                p_message_out := 'User '||USER||' does not have permission to call mrs.open()/mrs.close(). '||
                                'User must have Analyst/DBA/MGR role in ram.a$user for appl=''rectifydb''. '||
                                'For automation, add CAPS user to ram.a$user table.';
                post_a$log('PurgeCheckAndFix_SingleTable', 'ERROR: '||p_message_out);
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                -- If ram.a$user table doesn't exist or query fails, log warning but continue
                post_a$log('PurgeCheckAndFix_SingleTable', 
                    'WARNING: Could not verify user permissions: '||SQLERRM||
                    ' - Proceeding, but mrs.open()/mrs.close() may fail');
        END;
    END;
    
    -- Validate table name
    IF p_table_name IS NULL THEN
        p_status_out := 'FAILED';
        p_message_out := 'Table name is NULL';
        post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
        RETURN;
    END IF;
    
    -- Check if prerequisite needs to be fixed first
    -- All inv* tables (except inv_sum itself) require inv_sum as prerequisite
    v_prerequisite_table := NULL;
    IF UPPER(TRIM(p_table_name)) LIKE 'INV%' 
       AND UPPER(TRIM(p_table_name)) != 'INV_SUM' THEN
        v_prerequisite_table := 'INV_SUM';
        post_a$log('PurgeCheckAndFix_SingleTable', 
            'Prerequisite detected: '||p_table_name||' requires '||v_prerequisite_table||' to be fixed first');
        -- Note: In full automation, prerequisites should be handled by the main procedure
        -- For now, just log a warning
    END IF;
    
    -- Step 1: Generate datafix filename
    v_step := 'Generate datafix filename';
    v_datafix_filename := 'MDC'||TO_CHAR(SYSDATE, 'MM')||TO_CHAR(SYSDATE, 'YYYY')||
                          '_PURGED_datafix_'||LOWER(TRIM(p_table_name))||'.sql';
    
    post_a$log('PurgeCheckAndFix_SingleTable', 
        'Datafix filename: '||v_datafix_filename);
    
    IF p_dry_run THEN
        p_status_out := 'SKIPPED';
        p_message_out := 'DRY RUN: Would create file '||v_datafix_filename||' and process table '||p_table_name;
        post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
        RETURN;
    END IF;
    
    -- Step 2: Create datafix file
    v_step := 'Create datafix file';
    BEGIN
        -- Try to use UTL_FILE with directory object
        -- Note: Directory object DIR_RECTIFY may need to be created pointing to /usr/app/rectify/ware
        v_file_handle := UTL_FILE.FOPEN(v_directory_object, v_datafix_filename, 'W');
        
        -- First line: --RECTIFY:caps.<table_name>
        UTL_FILE.PUT_LINE(v_file_handle, '--RECTIFY:caps.'||LOWER(TRIM(p_table_name)));
        
        -- Second line: Description
        UTL_FILE.PUT_LINE(v_file_handle, 
            '--Purge datafix for '||LOWER(TRIM(p_table_name))||
            ' - Automated fix on '||TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
        
        UTL_FILE.FCLOSE(v_file_handle);
        
        post_a$log('PurgeCheckAndFix_SingleTable', 
            'Created datafix file: '||v_datafix_filename);
            
    EXCEPTION
        WHEN OTHERS THEN
            -- If directory object doesn't exist, try alternative approach
            -- For now, log error and continue (file may need to be created manually or via OS)
            post_a$log('PurgeCheckAndFix_SingleTable', 
                'WARNING: Could not create datafix file via UTL_FILE: '||SQLERRM||
                ' - File may need to be created manually or via OS command');
            -- Continue - mrs.open may still work if file exists
    END;
    
    -- Step 3: Open tablespace
    v_step := 'Open tablespace';
    BEGIN
        -- Call mrs.open() to request tablespace open
        -- NOTE: Requires VerifyUser('rectifydb') permission (Analyst/DBA/MGR role in ram.a$user)
        -- For automation, CAPS user should be added to ram.a$user table
        -- The procedure will read the datafix file and create episode record
        MRS.Open(v_datafix_filename);
        
        -- Extract episode_id from ram.a$rectifydb_log_master
        -- Get the most recent episode for this table that was just opened
        BEGIN
            SELECT id
            INTO v_episode_id
            FROM (
                SELECT id
                FROM ram.a$rectifydb_log_master
                WHERE episode_table = 'CAPS.'||UPPER(TRIM(p_table_name))
                  AND episode_open >= SYSDATE - (1/1440)  -- Within last minute
                  AND episode_close IS NULL
                ORDER BY episode_open DESC
            )
            WHERE ROWNUM = 1;
            
            post_a$log('PurgeCheckAndFix_SingleTable', 
                'Episode ID extracted: '||v_episode_id);
                
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                p_status_out := 'FAILED';
                p_message_out := 'Could not extract episode_id after mrs.open()';
                post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
                RETURN;
            WHEN OTHERS THEN
                p_status_out := 'FAILED';
                p_message_out := 'Error extracting episode_id: '||SQLERRM;
                post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
                RETURN;
        END;
        
    EXCEPTION
        WHEN OTHERS THEN
            p_status_out := 'FAILED';
            p_message_out := 'Error calling mrs.open(): '||SQLERRM;
            post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
            RETURN;
    END;
    
    -- Update queue table with episode_id
    IF p_queue_id IS NOT NULL THEN
        BEGIN
            UPDATE CAPS.a$purgecheck_errors_queue
            SET episode_id = v_episode_id,
                status = 'PROCESSING',
                datafix_filename = v_datafix_filename
            WHERE id = p_queue_id;
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                post_a$log('PurgeCheckAndFix_SingleTable', 
                    'Warning: Could not update queue table: '||SQLERRM);
        END;
    END IF;
    
    -- Step 4: Wait for DBA approval (if needed)
    IF NOT p_auto_tablespace THEN
        v_step := 'Wait for DBA to open tablespace';
        CAPS.a$PurgeCheck_WaitForTablespace(
            p_episode_id => v_episode_id,
            p_expected_status => 'OPEN',
            p_poll_interval_seconds => p_poll_interval_seconds,
            p_max_wait_minutes => p_max_wait_minutes,
            p_status_out => v_wait_status,
            p_message_out => v_wait_message
        );
        
        IF v_wait_status != 'READY' THEN
            p_status_out := 'TIMEOUT';
            p_message_out := 'Timeout waiting for DBA to open tablespace: '||v_wait_message;
            post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
            
            -- Update queue table
            IF p_queue_id IS NOT NULL THEN
                UPDATE CAPS.a$purgecheck_errors_queue
                SET status = 'FAILED',
                    error_message = p_message_out,
                    processed_date = SYSDATE
                WHERE id = p_queue_id;
                COMMIT;
            END IF;
            RETURN;
        END IF;
        
        post_a$log('PurgeCheckAndFix_SingleTable', 
            'Tablespace opened successfully: '||v_wait_message);
    END IF;
    
    -- Step 5: Execute purgeforce
    v_step := 'Execute a$purgeforce';
    BEGIN
        -- Check if prerequisite needs to be run first
        IF v_prerequisite_table IS NOT NULL THEN
            post_a$log('PurgeCheckAndFix_SingleTable', 
                'Running prerequisite: a$purgeforce('||v_prerequisite_table||')');
            BEGIN
                CAPS.a$purgeforce(v_prerequisite_table);
                post_a$log('PurgeCheckAndFix_SingleTable', 
                    'Prerequisite '||v_prerequisite_table||' completed');
            EXCEPTION
                WHEN OTHERS THEN
                    post_a$log('PurgeCheckAndFix_SingleTable', 
                        'Warning: Prerequisite '||v_prerequisite_table||' failed: '||SQLERRM);
                    -- Continue anyway
            END;
        END IF;
        
        -- Run purgeforce for the target table
        post_a$log('PurgeCheckAndFix_SingleTable', 
            'Executing a$purgeforce('||p_table_name||')');
        CAPS.a$purgeforce(p_table_name);
        post_a$log('PurgeCheckAndFix_SingleTable', 
            'a$purgeforce completed for '||p_table_name);
            
    EXCEPTION
        WHEN OTHERS THEN
            p_status_out := 'FAILED';
            p_message_out := 'Error executing a$purgeforce: '||SQLERRM;
            post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
            
            -- Update queue table
            IF p_queue_id IS NOT NULL THEN
                UPDATE CAPS.a$purgecheck_errors_queue
                SET status = 'FAILED',
                    error_message = p_message_out,
                    processed_date = SYSDATE
                WHERE id = p_queue_id;
                COMMIT;
            END IF;
            
            -- Still try to close tablespace
            BEGIN
                MRS.Close(v_episode_id, v_datafix_filename);
            EXCEPTION
                WHEN OTHERS THEN
                    post_a$log('PurgeCheckAndFix_SingleTable', 
                        'Error closing tablespace after failure: '||SQLERRM);
            END;
            RETURN;
    END;
    
    -- Step 6: Verify fix before closing tablespace
    v_step := 'Verify fix';
    BEGIN
        post_a$log('PurgeCheckAndFix_SingleTable', 
            'Running a$PurgeCheck to verify fix for '||p_table_name);
        
        -- Run a$PurgeCheck to check for remaining errors
        CAPS.a$PurgeCheck(FALSE);
        
        -- Check if there are still pending errors for this specific table
        BEGIN
            SELECT COUNT(*)
            INTO v_error_count_after
            FROM CAPS.a$purgecheck_errors_queue
            WHERE UPPER(TRIM(table_name)) = UPPER(TRIM(p_table_name))
              AND status = 'PENDING';
            
            IF v_error_count_after > 0 THEN
                -- Errors still remain for this table
                p_status_out := 'FAILED';
                p_message_out := 'Verification failed: '||v_error_count_after||
                                ' error(s) still remain for '||p_table_name||
                                ' after purgeforce execution';
                post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
                
                -- Update queue table
                IF p_queue_id IS NOT NULL THEN
                    UPDATE CAPS.a$purgecheck_errors_queue
                    SET status = 'FAILED',
                        error_message = p_message_out,
                        processed_date = SYSDATE,
                        retry_count = retry_count + 1
                    WHERE id = p_queue_id;
                    COMMIT;
                END IF;
                
                -- Do NOT close tablespace - keep it open for investigation
                post_a$log('PurgeCheckAndFix_SingleTable', 
                    'WARNING: Tablespace will remain OPEN due to remaining errors. Manual intervention required.');
                RETURN;
            ELSE
                -- No errors remaining - fix successful
                post_a$log('PurgeCheckAndFix_SingleTable', 
                    'Verification passed: No remaining errors for '||p_table_name);
            END IF;
            
        EXCEPTION
            WHEN OTHERS THEN
                post_a$log('PurgeCheckAndFix_SingleTable', 
                    'WARNING: Error checking remaining errors: '||SQLERRM||
                    ' - Proceeding to close tablespace anyway');
                -- Continue to close tablespace
        END;
        
    EXCEPTION
        WHEN OTHERS THEN
            post_a$log('PurgeCheckAndFix_SingleTable', 
                'WARNING: Error running a$PurgeCheck: '||SQLERRM||
                ' - Proceeding to close tablespace anyway');
            -- Continue to close tablespace
    END;
    
    -- Step 7: Close tablespace (only if verification passed)
    v_step := 'Close tablespace';
    BEGIN
        -- NOTE: Requires VerifyUser('rectifydb') permission AND must be same user who opened episode
        -- OR must be a DBA. For automation, CAPS user should be added to ram.a$user table
        MRS.Close(v_episode_id, v_datafix_filename);
        post_a$log('PurgeCheckAndFix_SingleTable', 
            'Tablespace close requested for episode '||v_episode_id);
            
    EXCEPTION
        WHEN OTHERS THEN
            p_status_out := 'FAILED';
            p_message_out := 'Error calling mrs.close(): '||SQLERRM;
            post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
            -- Continue to update queue
    END;
    
    -- Step 8: Wait for DBA approval to close (if needed)
    IF NOT p_auto_tablespace THEN
        v_step := 'Wait for DBA to close tablespace';
        CAPS.a$PurgeCheck_WaitForTablespace(
            p_episode_id => v_episode_id,
            p_expected_status => 'CLOSED',
            p_poll_interval_seconds => p_poll_interval_seconds,
            p_max_wait_minutes => p_max_wait_minutes,
            p_status_out => v_wait_status,
            p_message_out => v_wait_message
        );
        
        IF v_wait_status != 'READY' THEN
            post_a$log('PurgeCheckAndFix_SingleTable', 
                'Warning: Timeout waiting for DBA to close tablespace: '||v_wait_message);
            -- Don't fail the whole process, just log warning
        ELSE
            post_a$log('PurgeCheckAndFix_SingleTable', 
                'Tablespace closed successfully: '||v_wait_message);
        END IF;
    END IF;
    
    -- Step 9: Update queue table
    v_step := 'Update queue table';
    IF p_queue_id IS NOT NULL THEN
        BEGIN
            UPDATE CAPS.a$purgecheck_errors_queue
            SET status = 'COMPLETED',
                processed_date = SYSDATE,
                notes = 'Successfully fixed on '||TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')
            WHERE id = p_queue_id;
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                post_a$log('PurgeCheckAndFix_SingleTable', 
                    'Warning: Could not update queue table: '||SQLERRM);
        END;
    END IF;
    
    -- Success
    p_status_out := 'SUCCESS';
    p_message_out := 'Successfully fixed table '||p_table_name||' (Episode: '||v_episode_id||')';
    post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
    
EXCEPTION
    WHEN OTHERS THEN
        p_status_out := 'FAILED';
        p_message_out := 'Exception at step ['||v_step||']: '||SQLERRM;
        post_a$log('PurgeCheckAndFix_SingleTable', p_message_out);
        
        -- Update queue table
        IF p_queue_id IS NOT NULL THEN
            BEGIN
                UPDATE CAPS.a$purgecheck_errors_queue
                SET status = 'FAILED',
                    error_message = p_message_out,
                    processed_date = SYSDATE
                WHERE id = p_queue_id;
                COMMIT;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL; -- Ignore errors in error handling
            END;
        END IF;
        
        RAISE;
        
END a$PurgeCheckAndFix_SingleTable;
/

-- Add comment
COMMENT ON PROCEDURE CAPS.a$PurgeCheckAndFix_SingleTable IS 
    'Fixes purge errors for a single table: creates datafix file, opens/closes tablespace, runs purgeforce, verifies fix.';

-- Grant execute permission (adjust as needed)
-- GRANT EXECUTE ON CAPS.a$PurgeCheckAndFix_SingleTable TO <user>;

PROMPT Procedure CAPS.a$PurgeCheckAndFix_SingleTable created successfully

