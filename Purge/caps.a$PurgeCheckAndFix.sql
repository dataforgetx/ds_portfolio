------------------------------------------------------------------------------------------
-- Filename: caps.a$PurgeCheckAndFix.sql
--
-- Purpose: Main orchestration procedure for automated purge data fix.
--          Processes queue table, handles prerequisites, fixes errors, and verifies results.
--
------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE CAPS.a$PurgeCheckAndFix(
    p_auto_fix              IN BOOLEAN DEFAULT TRUE,
    p_auto_tablespace        IN BOOLEAN DEFAULT FALSE,
    p_process_queue         IN BOOLEAN DEFAULT TRUE,
    p_run_purgecheck_first  IN BOOLEAN DEFAULT TRUE,  -- Run a$PurgeCheck to collect errors first
    p_poll_interval_seconds  IN NUMBER DEFAULT 120,
    p_max_wait_minutes      IN NUMBER DEFAULT 240,
    p_dry_run               IN BOOLEAN DEFAULT FALSE
) IS
-----------------------------------------------------------------------------
-- Parameters:
--   p_auto_fix              - If TRUE, automatically fix errors (default TRUE)
--   p_auto_tablespace        - If TRUE, assumes tablespace auto-opens/closes
--   p_process_queue          - If TRUE, process queue table entries
--   p_run_purgecheck_first   - If TRUE, run a$PurgeCheck first to collect errors
--   p_poll_interval_seconds  - Polling interval for DBA approval
--   p_max_wait_minutes      - Max wait time for DBA approval
--   p_dry_run               - If TRUE, only validates, doesn't execute
--
-- Process:
--   1. Optionally run a$PurgeCheck to collect current errors
--   2. Query queue table for PENDING entries
--   3. Order tables (handle prerequisites: inv_sum before other inv*)
--   4. For each table: call a$PurgeCheckAndFix_SingleTable
--   5. Re-run a$PurgeCheck to verify all fixes
--   6. Generate summary report
--   7. Send email with results
-----------------------------------------------------------------------------

    -- Cursor for pending queue entries
    CURSOR c_pending_errors IS
        SELECT id,
               table_name,
               error_count,
               category,
               definition_table,
               detected_date,
               retry_count
        FROM CAPS.a$purgecheck_errors_queue
        WHERE status = 'PENDING'
        ORDER BY 
            -- Process inv_sum first, then other inv* tables, then others
            CASE 
                WHEN UPPER(table_name) = 'INV_SUM' THEN 1
                WHEN UPPER(table_name) LIKE 'INV%' THEN 2
                ELSE 3
            END,
            detected_date ASC;
    
    -- Statistics
    v_total_errors          NUMBER := 0;
    v_fixed_count           NUMBER := 0;
    v_failed_count          NUMBER := 0;
    v_skipped_count         NUMBER := 0;
    v_timeout_count         NUMBER := 0;
    v_start_time            DATE := SYSDATE;
    v_end_time              DATE;
    v_elapsed_minutes       NUMBER;
    
    -- Processing variables
    v_status                VARCHAR2(20);
    v_message               VARCHAR2(4000);
    v_summary_msg           VARCHAR2(40000) := '';
    v_errors_remaining      NUMBER := 0;
    v_tables_processed      NUMBER := 0;
    v_inv_sum_processed     BOOLEAN := FALSE;
    
    -- Error tracking
    TYPE ErrorTab IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
    v_error_list ErrorTab;
    v_error_idx  PLS_INTEGER := 0;
    
BEGIN
    
    post_a$log('PurgeCheckAndFix', '========================================');
    post_a$log('PurgeCheckAndFix', 'Starting automated purge data fix process');
    post_a$log('PurgeCheckAndFix', 'Auto Fix: '||CASE WHEN p_auto_fix THEN 'YES' ELSE 'NO' END);
    post_a$log('PurgeCheckAndFix', 'Auto Tablespace: '||CASE WHEN p_auto_tablespace THEN 'YES' ELSE 'NO' END);
    post_a$log('PurgeCheckAndFix', 'Process Queue: '||CASE WHEN p_process_queue THEN 'YES' ELSE 'NO' END);
    post_a$log('PurgeCheckAndFix', 'Dry Run: '||CASE WHEN p_dry_run THEN 'YES' ELSE 'NO' END);
    post_a$log('PurgeCheckAndFix', '========================================');
    
    -- Step 0: Reset any stuck PROCESSING entries (automated recovery)
    BEGIN
        DECLARE
            v_reset_count NUMBER;
            v_reset_list VARCHAR2(4000);
        BEGIN
            CAPS.a$PurgeCheck_ResetStuckEntries(
                p_hours_stuck => 24,  -- Reset entries stuck more than 24 hours
                p_reset_to_status => 'PENDING',  -- Reset to PENDING so they can be reprocessed
                p_dry_run => FALSE,
                p_reset_count => v_reset_count,
                p_reset_list => v_reset_list
            );
            
            IF v_reset_count > 0 THEN
                post_a$log('PurgeCheckAndFix', 
                    'Reset '||v_reset_count||' stuck PROCESSING entry/entries to PENDING');
                post_a$log('PurgeCheckAndFix', 'Reset entries: '||SUBSTR(v_reset_list, 1, 1000));
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                -- Don't fail if reset fails, just log warning
                post_a$log('PurgeCheckAndFix', 
                    'WARNING: Error resetting stuck entries: '||SQLERRM||' - continuing');
        END;
    END;
    
    -- Step 1: Run a$PurgeCheck to collect current errors (if requested)
    IF p_run_purgecheck_first THEN
        post_a$log('PurgeCheckAndFix', 'Step 1: Running a$PurgeCheck to collect errors...');
        BEGIN
            CAPS.a$PurgeCheck(FALSE);  -- Run with Debug = FALSE
            post_a$log('PurgeCheckAndFix', 'a$PurgeCheck completed - errors logged to queue');
        EXCEPTION
            WHEN OTHERS THEN
                post_a$log('PurgeCheckAndFix', 
                    'WARNING: Error running a$PurgeCheck: '||SQLERRM||' - continuing with existing queue');
        END;
    END IF;
    
    -- Step 2: Query queue for pending errors
    IF NOT p_process_queue THEN
        post_a$log('PurgeCheckAndFix', 'Queue processing disabled - exiting');
        RETURN;
    END IF;
    
    post_a$log('PurgeCheckAndFix', 'Step 2: Querying queue table for pending errors...');
    
    -- Count pending errors
    SELECT COUNT(*)
    INTO v_total_errors
    FROM CAPS.a$purgecheck_errors_queue
    WHERE status = 'PENDING';
    
    post_a$log('PurgeCheckAndFix', 'Found '||v_total_errors||' pending error(s) in queue');
    
    IF v_total_errors = 0 THEN
        post_a$log('PurgeCheckAndFix', 'No pending errors to process - exiting');
        v_summary_msg := 'No pending errors found in queue.';
        
        -- Still send summary email
        BEGIN
            caps.mail_pkg.send(
                p_from => 'PurgeCheckAndFix',
                p_to => caps.mail_pkg.Email_GetRecipients('tech'),
                p_cc => NULL,
                p_subject => 'PurgeCheckAndFix - No Errors',
                p_body => 'ODN: Automated purge fix process completed.'||chr(10)||
                         'No pending errors found in queue.'||chr(10)||
                         'Time: '||TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')
            );
        EXCEPTION
            WHEN OTHERS THEN
                post_a$log('PurgeCheckAndFix', 'Error sending email: '||SQLERRM);
        END;
        RETURN;
    END IF;
    
    -- Step 3: Process each pending error
    IF p_auto_fix THEN
        post_a$log('PurgeCheckAndFix', 'Step 3: Processing '||v_total_errors||' error(s)...');
        
        FOR rec IN c_pending_errors LOOP
            v_tables_processed := v_tables_processed + 1;
            
            post_a$log('PurgeCheckAndFix', 
                'Processing ['||v_tables_processed||'/'||v_total_errors||']: '||
                rec.table_name||' ('||rec.error_count||' errors, Category: '||rec.category||')');
            
            -- Check if this is inv_sum - mark it as processed
            IF UPPER(TRIM(rec.table_name)) = 'INV_SUM' THEN
                v_inv_sum_processed := TRUE;
            END IF;
            
            -- Check if this is another inv* table and inv_sum hasn't been processed yet
            IF UPPER(TRIM(rec.table_name)) LIKE 'INV%' 
               AND UPPER(TRIM(rec.table_name)) != 'INV_SUM'
               AND NOT v_inv_sum_processed THEN
                post_a$log('PurgeCheckAndFix', 
                    'WARNING: '||rec.table_name||' requires inv_sum, but inv_sum not in queue or not yet processed');
                -- Continue anyway - single table procedure will handle prerequisite
            END IF;
            
            -- Call single table fix procedure
            BEGIN
                CAPS.a$PurgeCheckAndFix_SingleTable(
                    p_table_name => rec.table_name,
                    p_queue_id => rec.id,
                    p_auto_tablespace => p_auto_tablespace,
                    p_poll_interval_seconds => p_poll_interval_seconds,
                    p_max_wait_minutes => p_max_wait_minutes,
                    p_dry_run => p_dry_run,
                    p_status_out => v_status,
                    p_message_out => v_message
                );
                
                -- Update statistics
                IF v_status = 'SUCCESS' THEN
                    v_fixed_count := v_fixed_count + 1;
                    post_a$log('PurgeCheckAndFix', 
                        'SUCCESS: '||rec.table_name||' - '||v_message);
                ELSIF v_status = 'FAILED' THEN
                    v_failed_count := v_failed_count + 1;
                    v_error_idx := v_error_idx + 1;
                    v_error_list(v_error_idx) := rec.table_name||': '||v_message;
                    post_a$log('PurgeCheckAndFix', 
                        'FAILED: '||rec.table_name||' - '||v_message);
                ELSIF v_status = 'TIMEOUT' THEN
                    v_timeout_count := v_timeout_count + 1;
                    v_error_idx := v_error_idx + 1;
                    v_error_list(v_error_idx) := rec.table_name||': '||v_message;
                    post_a$log('PurgeCheckAndFix', 
                        'TIMEOUT: '||rec.table_name||' - '||v_message);
                ELSIF v_status = 'SKIPPED' THEN
                    v_skipped_count := v_skipped_count + 1;
                    post_a$log('PurgeCheckAndFix', 
                        'SKIPPED: '||rec.table_name||' - '||v_message);
                END IF;
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_failed_count := v_failed_count + 1;
                    v_error_idx := v_error_idx + 1;
                    v_error_list(v_error_idx) := rec.table_name||': Exception - '||SQLERRM;
                    post_a$log('PurgeCheckAndFix', 
                        'EXCEPTION processing '||rec.table_name||': '||SQLERRM);
                    
                    -- Update queue table
                    BEGIN
                        UPDATE CAPS.a$purgecheck_errors_queue
                        SET status = 'FAILED',
                            error_message = 'Exception: '||SQLERRM,
                            processed_date = SYSDATE,
                            retry_count = retry_count + 1
                        WHERE id = rec.id;
                        COMMIT;
                    EXCEPTION
                        WHEN OTHERS THEN
                            NULL; -- Ignore errors in error handling
                    END;
            END;
            
        END LOOP;
        
        post_a$log('PurgeCheckAndFix', 
            'Processing complete: Fixed='||v_fixed_count||
            ', Failed='||v_failed_count||
            ', Timeout='||v_timeout_count||
            ', Skipped='||v_skipped_count);
    ELSE
        post_a$log('PurgeCheckAndFix', 'Auto-fix disabled - skipping processing');
    END IF;
    
    -- Step 4: Re-run a$PurgeCheck to verify fixes
    post_a$log('PurgeCheckAndFix', 'Step 4: Re-running a$PurgeCheck to verify fixes...');
    BEGIN
        CAPS.a$PurgeCheck(FALSE);
        
        -- Count remaining errors
        SELECT COUNT(*)
        INTO v_errors_remaining
        FROM CAPS.a$purgecheck_errors_queue
        WHERE status = 'PENDING';
        
        post_a$log('PurgeCheckAndFix', 
            'Verification complete: '||v_errors_remaining||' error(s) still pending');
            
    EXCEPTION
        WHEN OTHERS THEN
            post_a$log('PurgeCheckAndFix', 
                'WARNING: Error re-running a$PurgeCheck: '||SQLERRM);
    END;
    
    -- Step 5: Generate summary
    v_end_time := SYSDATE;
    v_elapsed_minutes := ROUND((v_end_time - v_start_time) * 1440, 2);
    
    v_summary_msg := 'Automated Purge Data Fix Process Summary'||chr(10)||
                     '========================================'||chr(10)||
                     'Start Time: '||TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS')||chr(10)||
                     'End Time: '||TO_CHAR(v_end_time, 'YYYY-MM-DD HH24:MI:SS')||chr(10)||
                     'Elapsed: '||v_elapsed_minutes||' minutes'||chr(10)||chr(10)||
                     'Statistics:'||chr(10)||
                     '  Total Errors Found: '||v_total_errors||chr(10)||
                     '  Tables Fixed: '||v_fixed_count||chr(10)||
                     '  Tables Failed: '||v_failed_count||chr(10)||
                     '  Timeouts: '||v_timeout_count||chr(10)||
                     '  Skipped: '||v_skipped_count||chr(10)||
                     '  Errors Remaining: '||v_errors_remaining||chr(10);
    
    -- Add error details if any
    IF v_error_idx > 0 THEN
        v_summary_msg := v_summary_msg||chr(10)||'Failed Tables:'||chr(10);
        FOR i IN 1..v_error_idx LOOP
            v_summary_msg := v_summary_msg||'  - '||v_error_list(i)||chr(10);
        END LOOP;
    END IF;
    
    -- Add recommendation
    IF v_errors_remaining > 0 THEN
        v_summary_msg := v_summary_msg||chr(10)||
                        'WARNING: '||v_errors_remaining||' error(s) still pending.'||chr(10)||
                        'Please review queue table and process manually if needed.';
    ELSIF v_fixed_count > 0 AND v_errors_remaining = 0 THEN
        v_summary_msg := v_summary_msg||chr(10)||
                        'SUCCESS: All errors have been fixed!';
    END IF;
    
    post_a$log('PurgeCheckAndFix', '========================================');
    post_a$log('PurgeCheckAndFix', 'Process completed');
    post_a$log('PurgeCheckAndFix', v_summary_msg);
    
    -- Step 6: Send summary email
    BEGIN
        caps.mail_pkg.send(
            p_from => 'PurgeCheckAndFix',
            p_to => caps.mail_pkg.Email_GetRecipients('tech'),
            p_cc => NULL,
            p_subject => 'PurgeCheckAndFix - Process Complete',
            p_body => 'ODN: '||v_summary_msg
        );
        post_a$log('PurgeCheckAndFix', 'Summary email sent');
    EXCEPTION
        WHEN OTHERS THEN
            post_a$log('PurgeCheckAndFix', 'Error sending email: '||SQLERRM);
    END;
    
EXCEPTION
    WHEN OTHERS THEN
        post_a$log('PurgeCheckAndFix', 'FATAL ERROR: '||SQLERRM);
        post_a$log('PurgeCheckAndFix', 'Process terminated abnormally');
        
        -- Try to send error email
        BEGIN
            caps.mail_pkg.send(
                p_from => 'PurgeCheckAndFix',
                p_to => caps.mail_pkg.Email_GetRecipients('tech'),
                p_cc => NULL,
                p_subject => 'PurgeCheckAndFix - FATAL ERROR',
                p_body => 'ODN: Fatal error in automated purge fix process:'||chr(10)||
                         SQLERRM||chr(10)||
                         'Time: '||TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')
            );
        EXCEPTION
            WHEN OTHERS THEN
                NULL; -- Ignore errors in error handling
        END;
        
        RAISE;
        
END a$PurgeCheckAndFix;
/

-- Add comment
COMMENT ON PROCEDURE CAPS.a$PurgeCheckAndFix IS 
    'Main orchestration procedure for automated purge data fix. Processes queue table, handles prerequisites, fixes errors, and verifies results.';

-- Grant execute permission (adjust as needed)
-- GRANT EXECUTE ON CAPS.a$PurgeCheckAndFix TO <user>;

PROMPT Procedure CAPS.a$PurgeCheckAndFix created successfully

