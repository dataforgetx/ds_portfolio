------------------------------------------------------------------------------------------
-- Filename: caps.a$PurgeCheck_WaitForTablespace.sql
--
-- Purpose: Procedure to wait/poll for DBA to open or close a tablespace.
--          Uses a$PurgeCheck_PollTablespace function with retry logic.
--
------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE CAPS.a$PurgeCheck_WaitForTablespace(
    p_episode_id            IN NUMBER,
    p_expected_status       IN VARCHAR2,  -- 'OPEN' or 'CLOSED'
    p_poll_interval_seconds IN NUMBER DEFAULT 120,
    p_max_wait_minutes      IN NUMBER DEFAULT 240,
    p_status_out            OUT VARCHAR2,  -- 'READY', 'TIMEOUT', 'ERROR'
    p_message_out           OUT VARCHAR2
) IS
-----------------------------------------------------------------------------
-- Parameters:
--   p_episode_id            - Episode ID from ram.a$rectifydb_log_master
--   p_expected_status       - Expected status: 'OPEN' or 'CLOSED'
--   p_poll_interval_seconds   - How often to check (in seconds)
--   p_max_wait_minutes      - Maximum time to wait (in minutes)
--   p_status_out            - Output: 'READY', 'TIMEOUT', or 'ERROR'
--   p_message_out           - Output: Status message
--
-- Purpose:
--   Polls for DBA to open/close tablespace with retry logic.
--   Sleeps between polls and times out after max wait period.
-----------------------------------------------------------------------------

    v_start_time        DATE := SYSDATE;
    v_end_time          DATE;
    v_elapsed_minutes   NUMBER;
    v_poll_count        NUMBER := 0;
    v_is_ready          BOOLEAN;
    v_sleep_seconds     NUMBER;
    
BEGIN
    
    -- Initialize outputs
    p_status_out := 'ERROR';
    p_message_out := '';
    
    -- Validate inputs
    IF p_episode_id IS NULL OR p_expected_status IS NULL THEN
        p_status_out := 'ERROR';
        p_message_out := 'Invalid parameters: episode_id or expected_status is NULL';
        RETURN;
    END IF;
    
    IF p_expected_status NOT IN ('OPEN', 'CLOSED') THEN
        p_status_out := 'ERROR';
        p_message_out := 'Invalid expected_status: must be OPEN or CLOSED';
        RETURN;
    END IF;
    
    IF p_poll_interval_seconds <= 0 OR p_max_wait_minutes <= 0 THEN
        p_status_out := 'ERROR';
        p_message_out := 'Invalid polling parameters: interval and max_wait must be > 0';
        RETURN;
    END IF;
    
    -- Calculate end time
    v_end_time := v_start_time + (p_max_wait_minutes / 1440); -- Convert minutes to days
    
    post_a$log('PurgeCheck_WaitForTablespace', 
        'Starting wait for episode '||p_episode_id||' to be '||p_expected_status||
        ' (max wait: '||p_max_wait_minutes||' minutes, poll interval: '||p_poll_interval_seconds||' seconds)');
    
    -- Polling loop
    LOOP
        v_poll_count := v_poll_count + 1;
        
        -- Check if tablespace is in expected status
        v_is_ready := CAPS.a$PurgeCheck_PollTablespace(
            p_episode_id => p_episode_id,
            p_expected_status => p_expected_status
        );
        
        IF v_is_ready THEN
            -- Success! Tablespace is ready
            v_elapsed_minutes := ROUND((SYSDATE - v_start_time) * 1440, 2);
            p_status_out := 'READY';
            p_message_out := 'Tablespace is '||p_expected_status||' after '||v_elapsed_minutes||' minutes ('||v_poll_count||' polls)';
            post_a$log('PurgeCheck_WaitForTablespace', p_message_out);
            RETURN;
        END IF;
        
        -- Check if we've exceeded max wait time
        IF SYSDATE >= v_end_time THEN
            v_elapsed_minutes := ROUND((SYSDATE - v_start_time) * 1440, 2);
            p_status_out := 'TIMEOUT';
            p_message_out := 'Timeout waiting for tablespace to be '||p_expected_status||
                           ' after '||v_elapsed_minutes||' minutes ('||v_poll_count||' polls)';
            post_a$log('PurgeCheck_WaitForTablespace', p_message_out);
            RETURN;
        END IF;
        
        -- Calculate sleep time (don't sleep past end time)
        v_sleep_seconds := LEAST(
            p_poll_interval_seconds,
            ROUND((v_end_time - SYSDATE) * 86400) -- Convert days to seconds
        );
        
        -- Sleep before next poll (using DBMS_LOCK.SLEEP)
        IF v_sleep_seconds > 0 THEN
            BEGIN
                DBMS_LOCK.SLEEP(v_sleep_seconds);
            EXCEPTION
                WHEN OTHERS THEN
                    -- If sleep fails, just continue (might be permission issue)
                    post_a$log('PurgeCheck_WaitForTablespace', 
                        'Warning: DBMS_LOCK.SLEEP failed: '||SQLERRM||' - continuing anyway');
            END;
        END IF;
        
        -- Log progress every 5 polls
        IF MOD(v_poll_count, 5) = 0 THEN
            v_elapsed_minutes := ROUND((SYSDATE - v_start_time) * 1440, 2);
            post_a$log('PurgeCheck_WaitForTablespace', 
                'Still waiting... Poll #'||v_poll_count||', elapsed: '||v_elapsed_minutes||' minutes');
        END IF;
        
    END LOOP;
    
EXCEPTION
    WHEN OTHERS THEN
        p_status_out := 'ERROR';
        p_message_out := 'Exception in wait procedure: '||SQLERRM;
        post_a$log('PurgeCheck_WaitForTablespace', p_message_out);
        RAISE;
        
END a$PurgeCheck_WaitForTablespace;
/

-- Add comment
COMMENT ON PROCEDURE CAPS.a$PurgeCheck_WaitForTablespace IS 
    'Waits/polls for DBA to open or close tablespace with retry logic. Returns status and message.';

-- Grant execute permission (adjust as needed)
-- GRANT EXECUTE ON CAPS.a$PurgeCheck_WaitForTablespace TO <user>;

PROMPT Procedure CAPS.a$PurgeCheck_WaitForTablespace created successfully

